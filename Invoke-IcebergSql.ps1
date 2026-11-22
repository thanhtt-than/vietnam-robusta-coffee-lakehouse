$script:TLCNRepoRoot = $PSScriptRoot
function Invoke-IcebergSql {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Query
    )

    $repoRoot = $script:TLCNRepoRoot
    $runner = Join-Path $repoRoot "run_spark.ps1"
    $relativeScript = "src/iceberg/_invoke_iceberg_sql_tmp.py"
    $scriptPath = Join-Path $repoRoot "src/iceberg/_invoke_iceberg_sql_tmp.py"

    if (!(Test-Path $runner)) {
        throw "Khong tim thay run_spark.ps1"
    }

    $encoded = [Convert]::ToBase64String(
        [Text.Encoding]::UTF8.GetBytes($Query)
    )

    $python = @"
import base64
from minio_direct import spark_session

sql = base64.b64decode("$encoded").decode("utf-8")

spark = spark_session("TLCN-Iceberg-SQL-Audit")

try:
    spark.sql(sql).show(200, truncate=False)
finally:
    spark.stop()
"@

    Set-Content -LiteralPath $scriptPath -Value $python -Encoding UTF8

    try {
        & $runner -Script $relativeScript

        if ($LASTEXITCODE -ne 0) {
            throw "Spark SQL execution failed"
        }
    }
    finally {
        Remove-Item -LiteralPath $scriptPath -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "INVOKE_ICEBERG_SQL_READY" -ForegroundColor Green
