# ConferenceProFlow — M3.2 local Kafka topic initialization.
# Run from any PowerShell working directory: .\infrastructure\init-kafka-topics.ps1
# Requires Docker Desktop, a root .env file, and an already running Kafka broker.
# Safe to repeat: existing topics are not deleted or recreated.

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$envPath = Join-Path $repoRoot '.env'
$composePath = Join-Path $PSScriptRoot 'compose.yaml'

if (-not (Test-Path -LiteralPath $envPath)) {
    throw "Missing $envPath. Copy .env.example to .env and configure local development credentials."
}

if (-not (Test-Path -LiteralPath $composePath)) {
    throw "Missing Docker Compose file: $composePath"
}

$composeArgs = @('compose', '--env-file', $envPath, '-f', $composePath, '--profile', 'kafka')
$bootstrap = 'localhost:19092' # Inside the Kafka container, not the Windows host.

Write-Host 'Checking Kafka broker availability...'
& docker @composeArgs exec -T kafka /opt/kafka/bin/kafka-cluster.sh api-versions --bootstrap-server $bootstrap | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw 'Kafka broker is not ready. Start it with: docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka up -d'
}

# The operational topic is reserved for actual RegistrationFinalizedV1 events.
# Keep manual CLI smoke records in the separate topic so tests do not poison analytics.
$topics = @(
    [pscustomobject]@{ Name = 'conferenceproflow.registration-finalized.v1'; Partitions = 3; RetentionMs = '604800000' },
    [pscustomobject]@{ Name = 'conferenceproflow.kafka-smoke.v1';        Partitions = 1; RetentionMs = '86400000'  }
)

foreach ($topic in $topics) {
    Write-Host "Ensuring topic $($topic.Name) exists..."
    & docker @composeArgs exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server $bootstrap --create --if-not-exists --topic $topic.Name --partitions $topic.Partitions --replication-factor 1 --config "retention.ms=$($topic.RetentionMs)"
    if ($LASTEXITCODE -ne 0) {
        throw "Topic creation failed: $($topic.Name)"
    }

    $description = & docker @composeArgs exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server $bootstrap --describe --topic $topic.Name
    if ($LASTEXITCODE -ne 0) {
        throw "Topic verification failed: $($topic.Name)"
    }

    $topicDescription = $description -join ' '
    if ($topicDescription -notmatch "PartitionCount:\s*$($topic.Partitions)\b") {
        throw "Unexpected partition count for $($topic.Name). Verify the existing topic manually; this script will not change partitions."
    }
    if ($topicDescription -notmatch 'ReplicationFactor:\s*1\b') {
        throw "Unexpected replication factor for $($topic.Name)."
    }

    # Verify the effective dynamic topic setting rather than silently assuming --if-not-exists updated it.
    $configs = & docker @composeArgs exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server $bootstrap --entity-type topics --entity-name $topic.Name --describe
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to inspect retention configuration for $($topic.Name)."
    }
    if (($configs -join ' ') -notmatch "retention\.ms=$($topic.RetentionMs)\b") {
        throw "Unexpected retention.ms for $($topic.Name). This script does not change pre-existing topic configuration."
    }

    Write-Host "Verified: $($topic.Name) | partitions=$($topic.Partitions) | replication=1 | retention.ms=$($topic.RetentionMs)"
}

Write-Host 'All Kafka topics verified. No existing messages or topics were deleted.'
