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


# --------------------------------------------------
# 1. Job arguments
# --------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_PATH",
        "TARGET_PATH",
        "QUARANTINE_PATH",
    ],
)


SOURCE_PATH = args["SOURCE_PATH"]

TARGET_PATH = args["TARGET_PATH"]

QUARANTINE_PATH = args[
    "QUARANTINE_PATH"
]


# --------------------------------------------------
# 2. Glue / Spark setup
# --------------------------------------------------

spark_context = (
    SparkContext.getOrCreate()
)

glue_context = GlueContext(
    spark_context
)

spark = (
    glue_context.spark_session
)


job = Job(
    glue_context
)

job.init(
    args["JOB_NAME"],
    args,
)


# --------------------------------------------------
# 3. Raw Bronze schema
# --------------------------------------------------

CUSTOMER_RAW_SCHEMA = StructType([
    StructField(
        "customer_id",
        StringType(),
        True,
    ),

    StructField(
        "first_name",
        StringType(),
        True,
    ),

    StructField(
        "last_name",
        StringType(),
        True,
    ),

    StructField(
        "email",
        StringType(),
        True,
    ),

    StructField(
        "country",
        StringType(),
        True,
    ),

    StructField(
        "created_at",
        StringType(),
        True,
    ),

    StructField(
        "updated_at",
        StringType(),
        True,
    ),
])


# --------------------------------------------------
# 4. Read Bronze
# --------------------------------------------------

print(
    f"Reading Bronze from: "
    f"{SOURCE_PATH}"
)


bronze_df = (
    spark.read
    .option(
        "header",
        True,
    )
    .schema(
        CUSTOMER_RAW_SCHEMA
    )
    .csv(
        SOURCE_PATH
    )
)


input_count = bronze_df.count()


print(
    f"Bronze row count: "
    f"{input_count}"
)


# --------------------------------------------------
# 5. Standardize raw values
# --------------------------------------------------

cleaned_df = (
    bronze_df

    # Preserve original timestamp text
    .withColumn(
        "created_at_raw",
        F.col("created_at")
    )

    .withColumn(
        "updated_at_raw",
        F.col("updated_at")
    )

    # Standardize string fields
    .withColumn(
        "customer_id",
        F.trim(
            F.col("customer_id")
        )
    )

    .withColumn(
        "first_name",
        F.trim(
            F.col("first_name")
        )
    )

    .withColumn(
        "last_name",
        F.trim(
            F.col("last_name")
        )
    )

    .withColumn(
        "email",
        F.lower(
            F.trim(
                F.col("email")
            )
        )
    )

    .withColumn(
        "country",
        F.trim(
            F.col("country")
        )
    )

    # Parse timestamps
    .withColumn(
        "created_at",
        F.to_timestamp(
            F.col("created_at")
        )
    )

    .withColumn(
        "updated_at",
        F.to_timestamp(
            F.col("updated_at")
        )
    )
)


# --------------------------------------------------
# 6. Build rejection reasons
# --------------------------------------------------

quality_df = cleaned_df.withColumn(
    "rejection_reason",

    F.concat_ws(
        "; ",

        F.when(
            F.col(
                "customer_id"
            ).isNull()
            |
            (
                F.col(
                    "customer_id"
                ) == ""
            ),

            F.lit(
                "customer_id_missing"
            )
        ),

        F.when(
            F.col(
                "first_name"
            ).isNull()
            |
            (
                F.col(
                    "first_name"
                ) == ""
            ),

            F.lit(
                "first_name_missing"
            )
        ),

        F.when(
            F.col(
                "last_name"
            ).isNull()
            |
            (
                F.col(
                    "last_name"
                ) == ""
            ),

            F.lit(
                "last_name_missing"
            )
        ),

        F.when(
            F.col(
                "email"
            ).isNull()
            |
            (
                F.col(
                    "email"
                ) == ""
            ),

            F.lit(
                "email_missing"
            )
        ),

        F.when(
            F.col(
                "country"
            ).isNull()
            |
            (
                F.col(
                    "country"
                ) == ""
            ),

            F.lit(
                "country_missing"
            )
        ),

        F.when(
            F.col(
                "created_at"
            ).isNull(),

            F.lit(
                "created_at_invalid"
            )
        ),

        F.when(
            F.col(
                "updated_at"
            ).isNull(),

            F.lit(
                "updated_at_invalid"
            )
        ),

        F.when(
            (
                F.col(
                    "created_at"
                ).isNotNull()
            )
            &
            (
                F.col(
                    "updated_at"
                ).isNotNull()
            )
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

            F.lit(
                "updated_at_before_created_at"
            )
        ),
    )
)


# --------------------------------------------------
# 7. Split valid and invalid records
# --------------------------------------------------

rule_invalid_df = quality_df.filter(
    F.col(
        "rejection_reason"
    ) != ""
)


valid_df = quality_df.filter(
    F.col(
        "rejection_reason"
    ) == ""
)


rule_invalid_count = (
    rule_invalid_df.count()
)


print(
    f"Rule-invalid rows: "
    f"{rule_invalid_count}"
)


# --------------------------------------------------
# 8. Deterministic duplicate handling
# --------------------------------------------------

customer_window = (
    Window
    .partitionBy(
        "customer_id"
    )
    .orderBy(
        F.col(
            "updated_at"
        ).desc_nulls_last()
    )
)


ranked_df = (
    valid_df

    .withColumn(
        "_row_number",

        F.row_number().over(
            customer_window
        )
    )
)


# Keep newest row per customer
silver_df = (
    ranked_df

    .filter(
        F.col(
            "_row_number"
        ) == 1
    )

    .drop(
        "_row_number",
        "rejection_reason",
        "created_at_raw",
        "updated_at_raw",
    )
)


# Older duplicate versions
duplicate_df = (
    ranked_df

    .filter(
        F.col(
            "_row_number"
        ) > 1
    )

    .drop(
        "_row_number"
    )

    .withColumn(
        "rejection_reason",
        F.lit(
            "duplicate_customer_id_older_version"
        )
    )
)


duplicate_count = (
    duplicate_df.count()
)


print(
    f"Duplicate rows quarantined: "
    f"{duplicate_count}"
)


# --------------------------------------------------
# 9. Combine quarantine records
# --------------------------------------------------

quarantine_df = (
    rule_invalid_df
    .unionByName(
        duplicate_df
    )
    .withColumn(
        "rejected_at",
        F.current_timestamp()
    )
)


quarantine_count = (
    quarantine_df.count()
)


silver_count = (
    silver_df.count()
)


print(
    f"Silver rows: "
    f"{silver_count}"
)

print(
    f"Quarantine rows: "
    f"{quarantine_count}"
)


# --------------------------------------------------
# 10. Reconciliation check
# --------------------------------------------------

if (
    input_count
    !=
    silver_count
    + quarantine_count
):

    raise ValueError(
        "Row reconciliation failed: "
        f"input={input_count}, "
        f"silver={silver_count}, "
        f"quarantine={quarantine_count}"
    )


print(
    "Row reconciliation passed."
)


# --------------------------------------------------
# 11. Show quarantine sample
# --------------------------------------------------

print(
    "Quarantine sample:"
)


quarantine_df.select(
    "customer_id",
    "first_name",
    "email",
    "created_at_raw",
    "updated_at_raw",
    "rejection_reason",
).show(
    20,
    truncate=False,
)


# --------------------------------------------------
# 12. Write Silver
# --------------------------------------------------

print(
    f"Writing Silver to: "
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


# --------------------------------------------------
# 13. Write Quarantine
# --------------------------------------------------

print(
    f"Writing Quarantine to: "
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


print(
    "Customers quality pipeline "
    "completed successfully."
)


job.commit()