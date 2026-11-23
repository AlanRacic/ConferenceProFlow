# Local-only, intentional duplicate-delivery demonstration for M3.4F.
# Reads an EXACT saved Registration Analytics Outbox Payload and sends one
# additional Kafka record with its ORIGINAL sessionId as the Kafka message key.
# No SQL data, Kafka offsets, topics, or consumer groups are reset/deleted.
# Only use on local development with synthetic/test data.
param(
    [Parameter(Mandatory = $true)]
    [string]$PayloadFile,

    [Parameter(Mandatory = $true)]
    [string]$ExpectedSessionId
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $PayloadFile -PathType Leaf)) {
    throw "Payload file not found: $PayloadFile"
}

$payload = (Get-Content -LiteralPath $PayloadFile -Raw -Encoding UTF8).Trim()
if ([string]::IsNullOrWhiteSpace($payload)) {
    throw 'Payload file is empty.'
}

# kafka-console-producer expects one single-line record with format key|JSON.
if ($payload.Contains("`n") -or $payload.Contains("`r") -or $payload.Contains('|')) {
    throw 'Expected one single-line JSON object without a pipe (|).'
}

try {
    $evt = $payload | ConvertFrom-Json
    $sessionId = ([Guid]::Parse([string]$evt.sessionId)).ToString('D')
    $messageId = ([Guid]::Parse([string]$evt.messageId)).ToString('D')
    $registrationId = ([Guid]::Parse([string]$evt.registrationId)).ToString('D')
    $expected = ([Guid]::Parse($ExpectedSessionId)).ToString('D')
} catch {
    throw 'Payload must be an original RegistrationFinalizedV1 JSON object with valid GUID properties.'
}

if ($sessionId -ne $expected) {
    throw "Session mismatch: payload=$sessionId, expected=$expected. Nothing sent."
}
if ($evt.status -notin @('confirmed', 'rejected')) {
    throw 'Event status must be confirmed or rejected. Nothing sent.'
}
$emptyGuid = ([Guid]::Empty).ToString('D')
if ($messageId -eq $emptyGuid -or $registrationId -eq $emptyGuid) {
    throw 'Event contains an empty identifier. Nothing sent.'
}

$record = "$sessionId|$payload"
Write-Host "Publishing one intentional DUPLICATE to LOCAL Kafka: session=$sessionId, message=$messageId"
$record | & docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic conferenceproflow.registration-finalized.v1 --property 'parse.key=true' --property 'key.separator=|'
if ($LASTEXITCODE -ne 0) {
    throw "Kafka console producer exited with code $LASTEXITCODE."
}
Write-Host 'Producer finished. Check Analytics Worker for DuplicateMessage and rerun the read-only SQL checks.'
