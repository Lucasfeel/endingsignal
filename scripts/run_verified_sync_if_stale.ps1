param(
    [double]$StaleAfterHours = 10,
    [switch]$SkipRequireAc
)

$ErrorActionPreference = "Stop"

function Get-DotEnvMap {
    param(
        [string]$Path
    )

    $values = @{}
    if (-not (Test-Path $Path)) {
        return $values
    }

    foreach ($line in Get-Content $Path) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#")) {
            continue
        }

        $parts = $trimmed -split "=", 2
        if ($parts.Count -ne 2) {
            continue
        }

        $key = $parts[0].Trim()
        $value = $parts[1].Trim()
        if (
            ($value.StartsWith('"') -and $value.EndsWith('"')) -or
            ($value.StartsWith("'") -and $value.EndsWith("'"))
        ) {
            $value = $value.Substring(1, $value.Length - 2)
        }

        $values[$key] = $value
    }

    return $values
}

function Merge-DotEnvMap {
    param(
        [hashtable]$Base,
        [hashtable]$Override
    )

    $merged = @{}
    if ($Base) {
        foreach ($key in $Base.Keys) {
            $merged[$key] = $Base[$key]
        }
    }
    if ($Override) {
        foreach ($key in $Override.Keys) {
            $merged[$key] = $Override[$key]
        }
    }
    return $merged
}

function Get-EffectiveValue {
    param(
        [string]$Name,
        [hashtable]$DotEnv,
        [string]$DefaultValue = $null
    )

    $envItem = Get-Item -Path "Env:$Name" -ErrorAction SilentlyContinue
    if ($envItem -and -not [string]::IsNullOrWhiteSpace($envItem.Value)) {
        return $envItem.Value
    }

    if ($DotEnv.ContainsKey($Name) -and -not [string]::IsNullOrWhiteSpace([string]$DotEnv[$Name])) {
        return [string]$DotEnv[$Name]
    }

    return $DefaultValue
}

function Apply-DatabaseEnvironment {
    param(
        [hashtable]$DotEnv
    )

    $verifiedSyncDatabaseUrl = Get-Item -Path "Env:VERIFIED_SYNC_DATABASE_URL" -ErrorAction SilentlyContinue
    if ($verifiedSyncDatabaseUrl -and -not [string]::IsNullOrWhiteSpace($verifiedSyncDatabaseUrl.Value)) {
        $env:DATABASE_URL = $verifiedSyncDatabaseUrl.Value
        return
    }

    $databaseUrl = Get-EffectiveValue -Name "DATABASE_URL" -DotEnv $DotEnv
    if (-not [string]::IsNullOrWhiteSpace($databaseUrl)) {
        $env:DATABASE_URL = $databaseUrl
        return
    }

    foreach ($name in @("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_TIMEZONE")) {
        $value = Get-EffectiveValue -Name $name -DotEnv $DotEnv
        if (-not [string]::IsNullOrWhiteSpace($value)) {
            Set-Item -Path "Env:$name" -Value $value
        }
    }
}

function Get-DatabaseEndpoint {
    param(
        [hashtable]$DotEnv
    )

    $databaseUrl = Get-EffectiveValue -Name "DATABASE_URL" -DotEnv $DotEnv
    if (-not [string]::IsNullOrWhiteSpace($databaseUrl)) {
        $normalizedUrl = $databaseUrl -replace "^postgresql://", "http://" -replace "^postgres://", "http://"
        try {
            $uri = [Uri]$normalizedUrl
            $port = if ($uri.Port -gt 0) { $uri.Port } else { 5432 }
            return @{
                Host = $uri.Host
                Port = $port
            }
        }
        catch {
        }
    }

    $portText = Get-EffectiveValue -Name "DB_PORT" -DotEnv $DotEnv -DefaultValue "5432"
    $port = 5432
    [void][int]::TryParse($portText, [ref]$port)

    return @{
        Host = Get-EffectiveValue -Name "DB_HOST" -DotEnv $DotEnv -DefaultValue "127.0.0.1"
        Port = $port
    }
}

function Test-TcpPort {
    param(
        [string]$TargetHost,
        [int]$Port,
        [int]$TimeoutMs = 1000
    )

    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $asyncResult = $client.BeginConnect($TargetHost, $Port, $null, $null)
        if (-not $asyncResult.AsyncWaitHandle.WaitOne($TimeoutMs, $false)) {
            return $false
        }

        $client.EndConnect($asyncResult)
        return $true
    }
    catch {
        return $false
    }
    finally {
        $client.Dispose()
    }
}

function Test-PythonDatabaseConnection {
    $probe = @'
from database import create_standalone_connection as c
conn = c()
conn.close()
'@
    try {
        $probe | python - *> $null
    }
    catch {
        return $false
    }
    return $LASTEXITCODE -eq 0
}

function Test-DailyCrawlerReportsTable {
    $probe = @'
from database import create_standalone_connection as c
conn = c()
cur = conn.cursor()
cur.execute("SELECT to_regclass('public.daily_crawler_reports')")
row = cur.fetchone()
conn.close()
raise SystemExit(0 if row and row[0] else 1)
'@
    try {
        $probe | python - *> $null
    }
    catch {
        return $false
    }
    return $LASTEXITCODE -eq 0
}

function Ensure-DatabaseSchema {
    if (Test-DailyCrawlerReportsTable) {
        return
    }

    Write-Host "DB schema is missing. Running init_db.py..."
    & python init_db.py
    if ($LASTEXITCODE -ne 0) {
        throw "init_db.py failed while preparing the local DB schema."
    }

    if (-not (Test-DailyCrawlerReportsTable)) {
        throw "Local DB schema is still incomplete after init_db.py."
    }
}

function Ensure-LocalDockerDb {
    param(
        [string]$TargetHost,
        [int]$Port
    )

    $localHosts = @("127.0.0.1", "localhost", "::1")
    if ($localHosts -notcontains $TargetHost) {
        return
    }

    if ((Test-TcpPort -TargetHost $TargetHost -Port $Port) -and (Test-PythonDatabaseConnection)) {
        Write-Host "Local DB endpoint $TargetHost`:$Port is already reachable."
        return
    }

    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw "DB endpoint $TargetHost`:$Port is local, but Docker is not available. Start the local Postgres service or point the sync to a reachable DB."
    }

    Write-Host "Local DB endpoint $TargetHost`:$Port is down. Starting Docker Compose db service..."
    & docker compose --profile with-db up -d db
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to start the Docker Compose db service."
    }

    $deadline = (Get-Date).AddMinutes(2)
    while ((Get-Date) -lt $deadline) {
        if ((Test-TcpPort -TargetHost $TargetHost -Port $Port) -and (Test-PythonDatabaseConnection)) {
            Write-Host "Local DB endpoint $TargetHost`:$Port is reachable."
            return
        }

        Start-Sleep -Seconds 2
    }

    & docker compose ps db
    throw "Docker Compose started, but Postgres did not become reachable at $TargetHost`:$Port within 2 minutes."
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    $baseDotEnv = Get-DotEnvMap -Path (Join-Path $repoRoot ".env")
    $overrideDotEnvPath = Join-Path $repoRoot ".env.verified-sync"
    $overrideDotEnv = Get-DotEnvMap -Path $overrideDotEnvPath
    $dotEnv = Merge-DotEnvMap -Base $baseDotEnv -Override $overrideDotEnv
    if ($overrideDotEnv.Count -gt 0) {
        Write-Host "Using verified-sync override config from $overrideDotEnvPath"
    }
    Apply-DatabaseEnvironment -DotEnv $dotEnv
    $endpoint = Get-DatabaseEndpoint -DotEnv $dotEnv
    Ensure-LocalDockerDb -TargetHost $endpoint.Host -Port $endpoint.Port
    Ensure-DatabaseSchema
    $pythonArgs = @(
        "run_verified_sync.py"
        "--if-stale"
        "--stale-after-hours"
        $StaleAfterHours.ToString([System.Globalization.CultureInfo]::InvariantCulture)
    )
    if (-not $SkipRequireAc) {
        $pythonArgs += "--require-ac"
    }
    if ($args.Count -gt 0) {
        $pythonArgs += $args
    }
    & python @pythonArgs
}
finally {
    Pop-Location
}
