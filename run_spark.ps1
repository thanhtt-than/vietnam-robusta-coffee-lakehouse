
param(
    [Parameter(Mandatory = $true)]
    [string]$Script,

    [Parameter(Mandatory = $false)]
    [string[]]$ScriptArgs = @()
)

$ErrorActionPreference = "Stop"

$Jars = @(
    "/workspace/infrastructure/iceberg/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar"
    "/workspace/infrastructure/iceberg/jars/iceberg-aws-bundle-1.10.1.jar"
) -join ","

# Chuan hoa duong dan script sang duong dan trong container
$RelativeScript = $Script.Replace("\", "/").TrimStart("/")

# Chan duong dan vuot ra ngoai /workspace
if ($RelativeScript -split "/" -contains "..") {
    throw "Script path must stay within /workspace"
}

$ContainerScript = "/workspace/$RelativeScript"

# Kiem tra file ton tai trong Docker
docker exec tlcn-spark test -f $ContainerScript

if ($LASTEXITCODE -ne 0) {
    throw "Script not found in container: $ContainerScript"
}

Write-Host "======================================"
Write-Host "TLCN Spark Runner"
Write-Host "Script : $ContainerScript"
Write-Host "Args   : $($ScriptArgs -join ' ')"
Write-Host "======================================"

# Tao mang tham so de tranh loi xu ly chuoi
$SparkCommand = @(
    "exec"
    "tlcn-spark"
    "/opt/spark/bin/spark-submit"
    "--master"
    "local[2]"
    "--jars"
    $Jars
    $ContainerScript
) + @($ScriptArgs)

& docker @SparkCommand

$ExitCode = $LASTEXITCODE

if ($ExitCode -ne 0) {
    throw "Spark job failed with exit code $ExitCode"
}

Write-Host "SPARK_JOB_PASSED" -ForegroundColor Green
