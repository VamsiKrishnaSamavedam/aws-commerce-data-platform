param(
    [Parameter(Mandatory = $true)][string]$Bucket,
    [Parameter(Mandatory = $true)][string]$ScriptKey,
    [Parameter(Mandatory = $true)][string]$LibraryKey,
    [string]$Region = "us-east-1"
)

$ErrorActionPreference = "Stop"
$role = "arn:aws:iam::036071343416:role/AWSGlueServiceRole-CommercePlatform"
$scriptLocation = "s3://$Bucket/$ScriptKey"
$libraryLocation = "s3://$Bucket/$LibraryKey"
$conf = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://$Bucket/silver_iceberg/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
$defaults = @{
    "--extra-py-files" = $libraryLocation
    "--enable-glue-datacatalog" = ""
    "--datalake-formats" = "iceberg"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--conf" = $conf
}
$jobs = @(
    "commerce-products-incremental-iceberg",
    "commerce-orders-incremental-iceberg",
    "commerce-order-items-incremental-iceberg",
    "commerce-payments-incremental-iceberg",
    "commerce-shipments-incremental-iceberg",
    "commerce-returns-incremental-iceberg"
)

foreach ($job in $jobs) {
    $payload = @{
        Role = $role
        GlueVersion = "5.0"
        Command = @{ Name = "glueetl"; PythonVersion = "3"; ScriptLocation = $scriptLocation }
        DefaultArguments = $defaults
        ExecutionProperty = @{ MaxConcurrentRuns = 1 }
        NumberOfWorkers = 10
        WorkerType = "G.1X"
    } | ConvertTo-Json -Depth 5 -Compress

    aws glue get-job --job-name $job --region $Region --no-cli-pager *> $null
    if ($LASTEXITCODE -eq 0) {
        aws glue update-job --job-name $job --job-update $payload --region $Region --no-cli-pager
    } else {
        aws glue create-job --name $job --cli-input-json $payload --region $Region --no-cli-pager
    }
    if ($LASTEXITCODE -ne 0) { throw "Unable to create or update $job" }
}
