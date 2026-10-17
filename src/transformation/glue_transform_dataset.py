import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window


# ============================================================
# 1. READ GLUE JOB PARAMETERS
# ============================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "DATASET_NAME",
        "BUCKET_NAME",
        "SOURCE_PATH",
    ],
)

DATASET_NAME = args["DATASET_NAME"]
BUCKET_NAME = args["BUCKET_NAME"]
SOURCE_PATH = args["SOURCE_PATH"]


# We calculate Silver and Quarantine paths automatically.

TARGET_PATH = (
    f"s3://{BUCKET_NAME}/"
    f"silver/{DATASET_NAME}/"
)

QUARANTINE_PATH = (
    f"s3://{BUCKET_NAME}/"
    f"quarantine/{DATASET_NAME}/"
)


# ============================================================
# 2. CREATE SPARK / GLUE CONTEXT
# ============================================================

spark_context = (
    SparkContext.getOrCreate()
)

glue_context = GlueContext(
    spark_context
)

spark = (
    glue_context.spark_session
)


# ============================================================
# 3. INITIALIZE GLUE JOB
# ============================================================

job = Job(
    glue_context
)

job.init(
    args["JOB_NAME"],
    args,
)


# ============================================================
# 4. HELPER FUNCTIONS
# ============================================================


def string_schema(columns):
    """
    Bronze data is initially read as strings.

    Why?

    Because Bronze may contain bad values.

    Example:

        quantity = "abc"
        created_at = "not-a-date"

    We want to preserve those values first,
    then validate them ourselves.
    """

    return StructType(
        [
            StructField(
                column,
                StringType(),
                True,
            )
            for column in columns
        ]
    )


def silver_path(dataset_name):
    """
    Build the Silver S3 path for another dataset.
    """

    return (
        f"s3://{BUCKET_NAME}/"
        f"silver/{dataset_name}/"
    )


def read_silver(dataset_name):
    """
    Read an existing Silver dataset.

    Example:

    orders transformation needs customers.

    Therefore:

        read_silver("customers")
    """

    return spark.read.parquet(
        silver_path(
            dataset_name
        )
    )


def is_blank(column_name):
    """
    Check if a string is NULL or empty.
    """

    return (
        F.col(
            column_name
        ).isNull()
        |
        (
            F.trim(
                F.col(
                    column_name
                )
            )
            == ""
        )
    )


def not_in_allowed_values(
    column_name,
    allowed_values,
):
    """
    Check whether a non-empty value is outside
    the accepted business domain.
    """

    return (
        F.col(
            column_name
        ).isNotNull()
        &
        (
            F.trim(
                F.col(
                    column_name
                )
            )
            != ""
        )
        &
        (
            ~F.col(
                column_name
            ).isin(
                *allowed_values
            )
        )
    )


def standardize_string(
    df,
    column_name,
    lowercase=False,
):
    """
    Trim whitespace.

    Optionally convert to lowercase.

    Example:

        "  TEST@EMAIL.COM  "

    becomes:

        "test@email.com"
    """

    expression = F.trim(
        F.col(
            column_name
        )
    )

    if lowercase:

        expression = F.lower(
            expression
        )

    return df.withColumn(
        column_name,
        expression,
    )


def add_quality_rules(
    df,
    rules,
):
    """
    Apply multiple data-quality rules.

    One row can fail more than one rule.

    Example:

        customer_id_missing;
        email_missing
    """

    reason_expressions = []

    for condition, reason in rules:

        reason_expressions.append(
            F.when(
                condition,
                F.lit(
                    reason
                ),
            )
        )

    return df.withColumn(
        "rejection_reason",
        F.concat_ws(
            "; ",
            *reason_expressions,
        ),
    )


# ============================================================
# 5. DEDUPLICATION HELPERS
# ============================================================


def build_dedupe_order(
    valid_df,
    dedupe_order,
):
    """
    Build deterministic duplicate ordering.

    Customers:
        updated_at DESC

    Other datasets:
        stable row hash

    The row hash acts as a deterministic
    tie breaker.
    """

    hash_columns = [
        F.coalesce(
            F.col(
                column
            ).cast(
                "string"
            ),
            F.lit(
                ""
            ),
        )
        for column
        in sorted(
            valid_df.columns
        )
        if column
        != "rejection_reason"
    ]


    valid_df = (
        valid_df
        .withColumn(
            "_row_hash",

            F.sha2(
                F.concat_ws(
                    "||",
                    *hash_columns,
                ),
                256,
            ),
        )
    )


    order_expressions = []


    for (
        column_name,
        direction,
    ) in dedupe_order:

        if (
            direction.lower()
            == "desc"
        ):

            order_expressions.append(
                F.col(
                    column_name
                ).desc_nulls_last()
            )

        elif (
            direction.lower()
            == "asc"
        ):

            order_expressions.append(
                F.col(
                    column_name
                ).asc_nulls_last()
            )

        else:

            raise ValueError(
                "Unsupported "
                "dedupe direction: "
                f"{direction}"
            )


    # Always use hash as final tie breaker.

    order_expressions.append(
        F.col(
            "_row_hash"
        ).desc()
    )


    return (
        valid_df,
        order_expressions,
    )


def deduplicate_and_split(
    df,
    primary_keys,
    dedupe_order=None,
    silver_drop_columns=None,
):
    """
    Produce:

    1. Silver records
    2. Rule-invalid quarantine records
    3. Duplicate quarantine records
    """

    if dedupe_order is None:

        dedupe_order = []


    if silver_drop_columns is None:

        silver_drop_columns = []


    # --------------------------------------------------------
    # Records that already failed business rules
    # --------------------------------------------------------

    rule_invalid_df = (
        df
        .filter(
            F.col(
                "rejection_reason"
            )
            != ""
        )
    )


    # --------------------------------------------------------
    # Records eligible for Silver
    # --------------------------------------------------------

    valid_df = (
        df
        .filter(
            F.col(
                "rejection_reason"
            )
            == ""
        )
    )


    # --------------------------------------------------------
    # Add deterministic duplicate ordering
    # --------------------------------------------------------

    (
        valid_df,
        order_expressions,
    ) = build_dedupe_order(
        valid_df,
        dedupe_order,
    )


    duplicate_window = (
        Window
        .partitionBy(
            *primary_keys
        )
        .orderBy(
            *order_expressions
        )
    )


    ranked_df = (
        valid_df
        .withColumn(
            "_row_number",

            F.row_number().over(
                duplicate_window
            ),
        )
    )


    # --------------------------------------------------------
    # Keep best record in Silver
    # --------------------------------------------------------

    silver_df = (
        ranked_df

        .filter(
            F.col(
                "_row_number"
            )
            == 1
        )

        .drop(
            "_row_number",
            "_row_hash",
            "rejection_reason",
            *silver_drop_columns,
        )
    )


    # --------------------------------------------------------
    # Older / duplicate records → quarantine
    # --------------------------------------------------------

    duplicate_df = (
        ranked_df

        .filter(
            F.col(
                "_row_number"
            )
            > 1
        )

        .drop(
            "_row_number",
            "_row_hash",
        )

        .withColumn(
            "rejection_reason",

            F.lit(
                "duplicate_primary_key_older_or_lower_priority_version"
            ),
        )
    )


    # --------------------------------------------------------
    # Combine all quarantine rows
    # --------------------------------------------------------

    quarantine_df = (
        rule_invalid_df

        .unionByName(
            duplicate_df,
            allowMissingColumns=True,
        )

        .withColumn(
            "rejected_at",
            F.current_timestamp(),
        )
    )


    return (
        silver_df,
        quarantine_df,
    )


# ============================================================
# 6. CUSTOMERS
# ============================================================


def transform_customers(
    raw_df,
):

    df = raw_df


    # Standardize strings.

    for column in [
        "customer_id",
        "first_name",
        "last_name",
        "country",
        "created_at",
        "updated_at",
    ]:

        df = standardize_string(
            df,
            column,
        )


    # Email gets lowercase treatment.

    df = standardize_string(
        df,
        "email",
        lowercase=True,
    )


    # Preserve raw timestamps before parsing.

    df = (
        df

        .withColumn(
            "created_at_raw",
            F.col(
                "created_at"
            ),
        )

        .withColumn(
            "updated_at_raw",
            F.col(
                "updated_at"
            ),
        )

        .withColumn(
            "created_at",

            F.expr(
                "try_to_timestamp("
                "created_at_raw)"
            ),
        )

        .withColumn(
            "updated_at",

            F.expr(
                "try_to_timestamp("
                "updated_at_raw)"
            ),
        )
    )


    rules = [

        (
            is_blank(
                "customer_id"
            ),
            "customer_id_missing",
        ),

        (
            is_blank(
                "first_name"
            ),
            "first_name_missing",
        ),

        (
            is_blank(
                "last_name"
            ),
            "last_name_missing",
        ),

        (
            is_blank(
                "email"
            ),
            "email_missing",
        ),

        (
            is_blank(
                "country"
            ),
            "country_missing",
        ),

        (
            F.col(
                "created_at"
            ).isNull(),

            "created_at_invalid",
        ),

        (
            F.col(
                "updated_at"
            ).isNull(),

            "updated_at_invalid",
        ),

        (
            F.col(
                "created_at"
            ).isNotNull()

            &

            F.col(
                "updated_at"
            ).isNotNull()

            &

            (
                F.col(
                    "updated_at"
                )
                <
                F.col(
                    "created_at"
                )
            ),

            "updated_at_before_created_at",
        ),
    ]


    return add_quality_rules(
        df,
        rules,
    )


# ============================================================
# 7. PRODUCTS
# ============================================================


def transform_products(
    raw_df,
):

    df = raw_df


    for column in [
        "product_id",
        "product_name",
        "category",
        "price",
    ]:

        df = standardize_string(
            df,
            column,
        )


    # Preserve raw price.

    df = (
        df

        .withColumn(
            "price_raw",
            F.col(
                "price"
            ),
        )

        .withColumn(
            "price",

            F.expr(
                "try_cast("
                "price_raw "
                "as decimal(12,2))"
            ),
        )
    )


    allowed_categories = [
        "Electronics",
        "Home",
        "Sports",
        "Books",
        "Clothing",
    ]


    rules = [

        (
            is_blank(
                "product_id"
            ),
            "product_id_missing",
        ),

        (
            is_blank(
                "product_name"
            ),
            "product_name_missing",
        ),

        (
            is_blank(
                "category"
            ),
            "category_missing",
        ),

        (
            not_in_allowed_values(
                "category",
                allowed_categories,
            ),
            "category_invalid",
        ),

        (
            F.col(
                "price"
            ).isNull(),
            "price_invalid",
        ),

        (
            F.col(
                "price"
            ).isNotNull()

            &

            (
                F.col(
                    "price"
                )
                <= 0
            ),

            "price_not_positive",
        ),
    ]


    return add_quality_rules(
        df,
        rules,
    )


# ============================================================
# 8. ORDERS
# ============================================================


def transform_orders(
    raw_df,
):

    df = raw_df


    for column in [
        "order_id",
        "customer_id",
        "order_date",
        "status",
        "total_amount",
    ]:

        df = standardize_string(
            df,
            column,
        )


    df = (
        df

        .withColumn(
            "order_date_raw",
            F.col(
                "order_date"
            ),
        )

        .withColumn(
            "total_amount_raw",
            F.col(
                "total_amount"
            ),
        )

        .withColumn(
            "order_date",

            F.expr(
                "try_to_timestamp("
                "order_date_raw)"
            ),
        )

        .withColumn(
            "total_amount",

            F.expr(
                "try_cast("
                "total_amount_raw "
                "as decimal(12,2))"
            ),
        )
    )


    # --------------------------------------------------------
    # Foreign-key validation:
    # orders.customer_id must exist in customers
    # --------------------------------------------------------

    customers = (
        read_silver(
            "customers"
        )

        .select(
            "customer_id"
        )

        .dropDuplicates()

        .withColumn(
            "_customer_exists",
            F.lit(
                True
            ),
        )
    )


    df = df.join(
        customers,
        on="customer_id",
        how="left",
    )


    allowed_statuses = [
        "completed",
        "shipped",
        "processing",
        "cancelled",
    ]


    rules = [

        (
            is_blank(
                "order_id"
            ),
            "order_id_missing",
        ),

        (
            is_blank(
                "customer_id"
            ),
            "customer_id_missing",
        ),

        (
            F.col(
                "_customer_exists"
            ).isNull(),
            "customer_not_found",
        ),

        (
            F.col(
                "order_date"
            ).isNull(),
            "order_date_invalid",
        ),

        (
            is_blank(
                "status"
            ),
            "status_missing",
        ),

        (
            not_in_allowed_values(
                "status",
                allowed_statuses,
            ),
            "status_invalid",
        ),

        (
            F.col(
                "total_amount"
            ).isNull(),
            "total_amount_invalid",
        ),

        (
            F.col(
                "total_amount"
            ).isNotNull()

            &

            (
                F.col(
                    "total_amount"
                )
                < 0
            ),

            "total_amount_negative",
        ),
    ]


    return add_quality_rules(
        df,
        rules,
    )


# ============================================================
# 9. ORDER ITEMS
# ============================================================


def transform_order_items(
    raw_df,
):

    df = raw_df


    for column in [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
    ]:

        df = standardize_string(
            df,
            column,
        )


    df = (
        df

        .withColumn(
            "quantity_raw",
            F.col(
                "quantity"
            ),
        )

        .withColumn(
            "unit_price_raw",
            F.col(
                "unit_price"
            ),
        )

        .withColumn(
            "quantity",

            F.expr(
                "try_cast("
                "quantity_raw "
                "as int)"
            ),
        )

        .withColumn(
            "unit_price",

            F.expr(
                "try_cast("
                "unit_price_raw "
                "as decimal(12,2))"
            ),
        )
    )


    # --------------------------------------------------------
    # Validate order foreign key
    # --------------------------------------------------------

    orders = (
        read_silver(
            "orders"
        )

        .select(
            "order_id"
        )

        .dropDuplicates()

        .withColumn(
            "_order_exists",
            F.lit(
                True
            ),
        )
    )


    # --------------------------------------------------------
    # Validate product foreign key
    # --------------------------------------------------------

    products = (
        read_silver(
            "products"
        )

        .select(
            "product_id"
        )

        .dropDuplicates()

        .withColumn(
            "_product_exists",
            F.lit(
                True
            ),
        )
    )


    df = (
        df

        .join(
            orders,
            on="order_id",
            how="left",
        )

        .join(
            products,
            on="product_id",
            how="left",
        )
    )


    rules = [

        (
            is_blank(
                "order_item_id"
            ),
            "order_item_id_missing",
        ),

        (
            is_blank(
                "order_id"
            ),
            "order_id_missing",
        ),

        (
            is_blank(
                "product_id"
            ),
            "product_id_missing",
        ),

        (
            F.col(
                "_order_exists"
            ).isNull(),
            "order_not_found",
        ),

        (
            F.col(
                "_product_exists"
            ).isNull(),
            "product_not_found",
        ),

        (
            F.col(
                "quantity"
            ).isNull(),
            "quantity_invalid",
        ),

        (
            F.col(
                "quantity"
            ).isNotNull()

            &

            (
                F.col(
                    "quantity"
                )
                <= 0
            ),

            "quantity_not_positive",
        ),

        (
            F.col(
                "unit_price"
            ).isNull(),
            "unit_price_invalid",
        ),

        (
            F.col(
                "unit_price"
            ).isNotNull()

            &

            (
                F.col(
                    "unit_price"
                )
                <= 0
            ),

            "unit_price_not_positive",
        ),
    ]


    return add_quality_rules(
        df,
        rules,
    )


# ============================================================
# 10. PAYMENTS
# ============================================================


def transform_payments(
    raw_df,
):

    df = raw_df


    for column in [
        "payment_id",
        "order_id",
        "payment_method",
        "amount",
        "payment_status",
        "payment_date",
    ]:

        df = standardize_string(
            df,
            column,
        )


    df = (
        df

        .withColumn(
            "amount_raw",
            F.col(
                "amount"
            ),
        )

        .withColumn(
            "payment_date_raw",
            F.col(
                "payment_date"
            ),
        )

        .withColumn(
            "amount",

            F.expr(
                "try_cast("
                "amount_raw "
                "as decimal(12,2))"
            ),
        )

        .withColumn(
            "payment_date",

            F.expr(
                "try_to_timestamp("
                "payment_date_raw)"
            ),
        )
    )


    orders = (
        read_silver(
            "orders"
        )

        .select(
            "order_id",

            F.col(
                "total_amount"
            ).alias(
                "_order_total"
            ),
        )

        .dropDuplicates(
            [
                "order_id"
            ]
        )

        .withColumn(
            "_order_exists",
            F.lit(
                True
            ),
        )
    )


    df = df.join(
        orders,
        on="order_id",
        how="left",
    )


    allowed_methods = [
        "credit_card",
        "debit_card",
        "paypal",
        "apple_pay",
        "gift_card",
    ]


    allowed_statuses = [
        "completed",
        "failed",
        "refunded",
    ]


    rules = [

        (
            is_blank(
                "payment_id"
            ),
            "payment_id_missing",
        ),

        (
            is_blank(
                "order_id"
            ),
            "order_id_missing",
        ),

        (
            F.col(
                "_order_exists"
            ).isNull(),
            "order_not_found",
        ),

        (
            is_blank(
                "payment_method"
            ),
            "payment_method_missing",
        ),

        (
            not_in_allowed_values(
                "payment_method",
                allowed_methods,
            ),
            "payment_method_invalid",
        ),

        (
            is_blank(
                "payment_status"
            ),
            "payment_status_missing",
        ),

        (
            not_in_allowed_values(
                "payment_status",
                allowed_statuses,
            ),
            "payment_status_invalid",
        ),

        (
            F.col(
                "amount"
            ).isNull(),
            "amount_invalid",
        ),

        (
            F.col(
                "amount"
            ).isNotNull()

            &

            (
                F.col(
                    "amount"
                )
                < 0
            ),

            "amount_negative",
        ),

        (
            F.col(
                "_order_exists"
            ).isNotNull()

            &

            F.col(
                "amount"
            ).isNotNull()

            &

            F.col(
                "_order_total"
            ).isNotNull()

            &

            (
                F.col(
                    "amount"
                )
                !=
                F.col(
                    "_order_total"
                )
            ),

            "payment_amount_order_total_mismatch",
        ),

        (
            F.col(
                "payment_date"
            ).isNull(),
            "payment_date_invalid",
        ),
    ]


    return add_quality_rules(
        df,
        rules,
    )


# ============================================================
# 11. SHIPMENTS
# ============================================================


def transform_shipments(
    raw_df,
):

    df = raw_df


    for column in [
        "shipment_id",
        "order_id",
        "carrier",
        "ship_date",
        "delivery_date",
        "status",
    ]:

        df = standardize_string(
            df,
            column,
        )


    df = (
        df

        .withColumn(
            "ship_date_raw",
            F.col(
                "ship_date"
            ),
        )

        .withColumn(
            "delivery_date_raw",
            F.col(
                "delivery_date"
            ),
        )

        .withColumn(
            "ship_date",

            F.expr(
                "try_to_timestamp("
                "ship_date_raw)"
            ),
        )

        .withColumn(
            "delivery_date",

            F.expr(
                "try_to_timestamp("
                "delivery_date_raw)"
            ),
        )
    )


    orders = (
        read_silver(
            "orders"
        )

        .select(
            "order_id",

            F.col(
                "order_date"
            ).alias(
                "_order_date"
            ),

            F.col(
                "status"
            ).alias(
                "_order_status"
            ),
        )

        .dropDuplicates(
            [
                "order_id"
            ]
        )

        .withColumn(
            "_order_exists",
            F.lit(
                True
            ),
        )
    )


    df = df.join(
        orders,
        on="order_id",
        how="left",
    )


    allowed_carriers = [
        "UPS",
        "FedEx",
        "USPS",
        "DHL",
    ]


    allowed_statuses = [
        "shipped",
        "delivered",
    ]


    rules = [

        (
            is_blank(
                "shipment_id"
            ),
            "shipment_id_missing",
        ),

        (
            is_blank(
                "order_id"
            ),
            "order_id_missing",
        ),

        (
            F.col(
                "_order_exists"
            ).isNull(),
            "order_not_found",
        ),

        (
            is_blank(
                "carrier"
            ),
            "carrier_missing",
        ),

        (
            not_in_allowed_values(
                "carrier",
                allowed_carriers,
            ),
            "carrier_invalid",
        ),

        (
            is_blank(
                "status"
            ),
            "shipment_status_missing",
        ),

        (
            not_in_allowed_values(
                "status",
                allowed_statuses,
            ),
            "shipment_status_invalid",
        ),

        (
            F.col(
                "ship_date"
            ).isNull(),
            "ship_date_invalid",
        ),

        (
            F.col(
                "_order_exists"
            ).isNotNull()

            &

            (
                ~F.col(
                    "_order_status"
                ).isin(
                    "completed",
                    "shipped",
                )
            ),

            "order_not_shippable",
        ),

        (
            F.col(
                "ship_date"
            ).isNotNull()

            &

            F.col(
                "_order_date"
            ).isNotNull()

            &

            (
                F.col(
                    "ship_date"
                )
                <
                F.col(
                    "_order_date"
                )
            ),

            "ship_date_before_order_date",
        ),

        (
            (
                F.col(
                    "status"
                )
                == "delivered"
            )

            &

            F.col(
                "delivery_date"
            ).isNull(),

            "delivery_date_missing",
        ),

        (
            F.col(
                "delivery_date"
            ).isNotNull()

            &

            F.col(
                "ship_date"
            ).isNotNull()

            &

            (
                F.col(
                    "delivery_date"
                )
                <
                F.col(
                    "ship_date"
                )
            ),

            "delivery_before_ship_date",
        ),
    ]


    return add_quality_rules(
        df,
        rules,
    )


# ============================================================
# 12. RETURNS
# ============================================================


def transform_returns(
    raw_df,
):

    df = raw_df


    for column in [
        "return_id",
        "order_id",
        "product_id",
        "return_reason",
        "return_date",
    ]:

        df = standardize_string(
            df,
            column,
        )


    df = (
        df

        .withColumn(
            "return_date_raw",
            F.col(
                "return_date"
            ),
        )

        .withColumn(
            "return_date",

            F.expr(
                "try_to_timestamp("
                "return_date_raw)"
            ),
        )
    )


    # --------------------------------------------------------
    # Validate actual order + product combination
    # --------------------------------------------------------

    purchased_items = (
        read_silver(
            "order_items"
        )

        .select(
            "order_id",
            "product_id",
        )

        .dropDuplicates()

        .withColumn(
            "_purchase_exists",
            F.lit(
                True
            ),
        )
    )


    # --------------------------------------------------------
    # Need shipment delivery date
    # --------------------------------------------------------

    shipments = (
        read_silver(
            "shipments"
        )

        .select(
            "order_id",
            "delivery_date",
        )

        .dropDuplicates(
            [
                "order_id"
            ]
        )

        .withColumn(
            "_shipment_exists",
            F.lit(
                True
            ),
        )
    )


    df = (
        df

        .join(
            purchased_items,

            on=[
                "order_id",
                "product_id",
            ],

            how="left",
        )

        .join(
            shipments,

            on="order_id",

            how="left",
        )
    )


    allowed_reasons = [
        "damaged",
        "wrong_item",
        "not_as_described",
        "changed_mind",
        "defective",
        "late_delivery",
    ]


    rules = [

        (
            is_blank(
                "return_id"
            ),
            "return_id_missing",
        ),

        (
            is_blank(
                "order_id"
            ),
            "order_id_missing",
        ),

        (
            is_blank(
                "product_id"
            ),
            "product_id_missing",
        ),

        (
            F.col(
                "_purchase_exists"
            ).isNull(),

            "product_not_purchased_in_order",
        ),

        (
            is_blank(
                "return_reason"
            ),
            "return_reason_missing",
        ),

        (
            not_in_allowed_values(
                "return_reason",
                allowed_reasons,
            ),

            "return_reason_invalid",
        ),

        (
            F.col(
                "return_date"
            ).isNull(),

            "return_date_invalid",
        ),

        (
            F.col(
                "_shipment_exists"
            ).isNull(),

            "shipment_not_found",
        ),

        (
            F.col(
                "_shipment_exists"
            ).isNotNull()

            &

            F.col(
                "delivery_date"
            ).isNull(),

            "order_not_delivered",
        ),

        (
            F.col(
                "return_date"
            ).isNotNull()

            &

            F.col(
                "delivery_date"
            ).isNotNull()

            &

            (
                F.col(
                    "return_date"
                )
                <
                F.col(
                    "delivery_date"
                )
            ),

            "return_before_delivery",
        ),
    ]


    return add_quality_rules(
        df,
        rules,
    )


# ============================================================
# 13. DATASET CONFIGURATION
# ============================================================

DATASET_CONFIG = {


    # --------------------------------------------------------
    # CUSTOMERS
    # --------------------------------------------------------

    "customers": {

        "columns": [
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "country",
            "created_at",
            "updated_at",
        ],

        "transform":
            transform_customers,

        "primary_keys": [
            "customer_id"
        ],

        # Important:
        # newest customer version wins.

        "dedupe_order": [
            (
                "updated_at",
                "desc",
            )
        ],

        "silver_drop_columns": [
            "created_at_raw",
            "updated_at_raw",
        ],
    },


    # --------------------------------------------------------
    # PRODUCTS
    # --------------------------------------------------------

    "products": {

        "columns": [
            "product_id",
            "product_name",
            "category",
            "price",
        ],

        "transform":
            transform_products,

        "primary_keys": [
            "product_id"
        ],

        "dedupe_order": [],

        "silver_drop_columns": [
            "price_raw",
        ],
    },


    # --------------------------------------------------------
    # ORDERS
    # --------------------------------------------------------

    "orders": {

        "columns": [
            "order_id",
            "customer_id",
            "order_date",
            "status",
            "total_amount",
        ],

        "transform":
            transform_orders,

        "primary_keys": [
            "order_id"
        ],

        "dedupe_order": [],

        "silver_drop_columns": [
            "order_date_raw",
            "total_amount_raw",
            "_customer_exists",
        ],
    },


    # --------------------------------------------------------
    # ORDER ITEMS
    # --------------------------------------------------------

    "order_items": {

        "columns": [
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
        ],

        "transform":
            transform_order_items,

        "primary_keys": [
            "order_item_id"
        ],

        "dedupe_order": [],

        "silver_drop_columns": [
            "quantity_raw",
            "unit_price_raw",
            "_order_exists",
            "_product_exists",
        ],
    },


    # --------------------------------------------------------
    # PAYMENTS
    # --------------------------------------------------------

    "payments": {

        "columns": [
            "payment_id",
            "order_id",
            "payment_method",
            "amount",
            "payment_status",
            "payment_date",
        ],

        "transform":
            transform_payments,

        "primary_keys": [
            "payment_id"
        ],

        "dedupe_order": [],

        "silver_drop_columns": [
            "amount_raw",
            "payment_date_raw",
            "_order_total",
            "_order_exists",
        ],
    },


    # --------------------------------------------------------
    # SHIPMENTS
    # --------------------------------------------------------

    "shipments": {

        "columns": [
            "shipment_id",
            "order_id",
            "carrier",
            "ship_date",
            "delivery_date",
            "status",
        ],

        "transform":
            transform_shipments,

        "primary_keys": [
            "shipment_id"
        ],

        "dedupe_order": [],

        "silver_drop_columns": [
            "ship_date_raw",
            "delivery_date_raw",
            "_order_date",
            "_order_status",
            "_order_exists",
        ],
    },


    # --------------------------------------------------------
    # RETURNS
    # --------------------------------------------------------

    "returns": {

        "columns": [
            "return_id",
            "order_id",
            "product_id",
            "return_reason",
            "return_date",
        ],

        "transform":
            transform_returns,

        "primary_keys": [
            "return_id"
        ],

        "dedupe_order": [],

        "silver_drop_columns": [
            "return_date_raw",
            "_purchase_exists",
            "_shipment_exists",
            "delivery_date",
        ],
    },
}


# ============================================================
# 14. VALIDATE DATASET PARAMETER
# ============================================================

if (
    DATASET_NAME
    not in
    DATASET_CONFIG
):

    raise ValueError(
        f"Unsupported dataset: "
        f"{DATASET_NAME}. "
        f"Supported datasets: "
        f"{list(DATASET_CONFIG.keys())}"
    )


config = DATASET_CONFIG[
    DATASET_NAME
]


# ============================================================
# 15. PRINT JOB CONFIGURATION
# ============================================================

print(
    "=" * 70
)

print(
    f"Dataset: "
    f"{DATASET_NAME}"
)

print(
    f"Bronze source: "
    f"{SOURCE_PATH}"
)

print(
    f"Silver target: "
    f"{TARGET_PATH}"
)

print(
    f"Quarantine target: "
    f"{QUARANTINE_PATH}"
)

print(
    "=" * 70
)


# ============================================================
# 16. READ BRONZE CSV
# ============================================================

raw_df = (
    spark.read

    .option(
        "header",
        True,
    )

    .schema(
        string_schema(
            config[
                "columns"
            ]
        )
    )

    .csv(
        SOURCE_PATH
    )
)


input_count = (
    raw_df.count()
)


print(
    f"Input rows: "
    f"{input_count}"
)


# ============================================================
# 17. TRANSFORM + VALIDATE
# ============================================================

quality_df = (
    config[
        "transform"
    ](
        raw_df
    )
)


# ============================================================
# 18. DEDUPLICATE + SPLIT
# ============================================================

(
    silver_df,
    quarantine_df,
) = deduplicate_and_split(

    df=quality_df,

    primary_keys=(
        config[
            "primary_keys"
        ]
    ),

    dedupe_order=(
        config[
            "dedupe_order"
        ]
    ),

    silver_drop_columns=(
        config[
            "silver_drop_columns"
        ]
    ),
)


# ============================================================
# 19. CACHE OUTPUTS
# ============================================================

# We count and then write these DataFrames.
#
# Without cache, Spark could recompute the entire
# transformation multiple times.

silver_df = (
    silver_df.cache()
)

quarantine_df = (
    quarantine_df.cache()
)


# ============================================================
# 20. COUNT OUTPUTS
# ============================================================

silver_count = (
    silver_df.count()
)

quarantine_count = (
    quarantine_df.count()
)


print(
    f"Silver rows: "
    f"{silver_count}"
)

print(
    f"Quarantine rows: "
    f"{quarantine_count}"
)


# ============================================================
# 21. ROW RECONCILIATION
# ============================================================

if (
    input_count
    !=
    (
        silver_count
        +
        quarantine_count
    )
):

    raise ValueError(
        "Row reconciliation failed: "
        f"input={input_count}, "
        f"silver={silver_count}, "
        f"quarantine="
        f"{quarantine_count}"
    )


print(
    "Row reconciliation passed."
)


# ============================================================
# 22. SHOW QUARANTINE SAMPLE
# ============================================================

if (
    quarantine_count
    > 0
):

    print(
        "Quarantine sample:"
    )


    display_columns = [

        column

        for column
        in [
            *config[
                "primary_keys"
            ],

            "rejection_reason",

            "rejected_at",
        ]

        if column
        in quarantine_df.columns
    ]


    (
        quarantine_df

        .select(
            *display_columns
        )

        .show(
            20,
            truncate=False,
        )
    )


# ============================================================
# 23. WRITE SILVER
# ============================================================

print(
    f"Writing Silver data to: "
    f"{TARGET_PATH}"
)


(
    silver_df

    .write

    .mode(
        "overwrite"
    )

    .parquet(
        TARGET_PATH
    )
)


# ============================================================
# 24. WRITE QUARANTINE
# ============================================================

print(
    f"Writing Quarantine data to: "
    f"{QUARANTINE_PATH}"
)


(
    quarantine_df

    .write

    .mode(
        "overwrite"
    )

    .parquet(
        QUARANTINE_PATH
    )
)


# ============================================================
# 25. COMPLETE
# ============================================================

print(
    f"{DATASET_NAME} "
    f"Bronze -> Silver "
    f"processing completed "
    f"successfully."
)


silver_df.unpersist()

quarantine_df.unpersist()


job.commit()