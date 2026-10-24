#!/usr/bin/env bash
# Deploy the market-hours implied-move job as an Azure Container Apps scheduled job.
#
#   export AZURE_POSTGRES_PASSWORD=...
#   deploy/azure/deploy_implied_moves_job.sh
#
# Why: options-implied earnings moves need live option quotes. After the close Yahoo returns bid = ask =
# openInterest = 0 on every contract, so the nightly updater (which only runs while the market is closed) stored NULL
# for 224 of 232 reports. This job runs database/implied_moves_job.py at :15 past every hour 13-20 UTC on weekdays;
# the code only works inside 09:45-15:45 New York (holidays included), so 6 runs a day do work - 10:15 ... 15:15 New
# York in summer and winter alike - and the others exit at once. It only reads option chains (~2.5 paced Yahoo
# requests per report in the next 14 days) and only writes earnings_calendar.implied_move_*. Each run stops cleanly
# after 50 minutes or 1,000 Yahoo requests, and all runs of a New York day share 3,500 requests (counted in
# cron_runs). Nearest reports go first; a stored move is re-quoted once a day, a thin or wide quote is retried once a
# day (every other run in the last 3 days before the report), a stock with no listed options once a week.
#
# It reuses the image and Container Apps environment of the workers (deploy_container_apps_job.sh), whose IP is idle
# during market hours. Deploy the workers first so the image contains database/implied_moves_job.py; this script
# never builds (IMAGE_TAG=<tag> picks a specific image, default: the newest).
set -euo pipefail

: "${AZURE_POSTGRES_PASSWORD:?export AZURE_POSTGRES_PASSWORD first}"

RESOURCE_GROUP="${RESOURCE_GROUP:-tickzen-db-exporter-rg}"
ACR_NAME="${ACR_NAME:-tickzendbexporter}"
IMAGE_REPO="tickzen-db-exporter"
LOCATION="${LOCATION:-centralindia}"
ENV_NAME="${ENV_NAME:-tickzen-exporter-${LOCATION}}"
JOB_NAME="${JOB_NAME:-tickzen-implied-moves}"
CRON_EXPRESSION="${CRON_EXPRESSION:-15 13-20 * * 1-5}"       # UTC; the job itself checks the New York session
REPLICA_TIMEOUT_SECONDS="${REPLICA_TIMEOUT_SECONDS:-3600}"
YAHOO_REQUESTS_PER_HOUR="${YAHOO_REQUESTS_PER_HOUR:-1200}"

PG_HOST="${AZURE_POSTGRES_HOST:-tickzen-stock-data-v2.postgres.database.azure.com}"
PG_DB="${AZURE_POSTGRES_DB:-tickzen-data}"
PG_USER="${AZURE_POSTGRES_USER:-jadaunkg}"
PG_PORT="${AZURE_POSTGRES_PORT:-5432}"

TAG="${IMAGE_TAG:-$(az acr repository show-tags -n "$ACR_NAME" --repository "$IMAGE_REPO" --orderby time_desc --top 1 -o tsv)}"
IMAGE="${ACR_NAME}.azurecr.io/${IMAGE_REPO}:${TAG}"
ACR_PASSWORD="$(az acr credential show -n "$ACR_NAME" --query 'passwords[0].value' -o tsv)"
ENV_ID="$(az containerapp env show -g "$RESOURCE_GROUP" -n "$ENV_NAME" --query id -o tsv)"
echo "==> ${JOB_NAME}: image ${IMAGE}, environment ${ENV_NAME}, cron '${CRON_EXPRESSION}' (UTC)"

yaml_str() { printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'; }

SPEC="$(mktemp)"
chmod 600 "$SPEC"
trap 'rm -f "$SPEC"' EXIT
cat > "$SPEC" <<EOF
location: ${LOCATION}
properties:
  environmentId: ${ENV_ID}
  configuration:
    triggerType: Schedule
    replicaTimeout: ${REPLICA_TIMEOUT_SECONDS}
    replicaRetryLimit: 0
    scheduleTriggerConfig:
      cronExpression: "${CRON_EXPRESSION}"
      parallelism: 1
      replicaCompletionCount: 1
    registries:
      - server: ${ACR_NAME}.azurecr.io
        username: ${ACR_NAME}
        passwordSecretRef: acr-password
    secrets:
      - name: acr-password
        value: "$(yaml_str "$ACR_PASSWORD")"
      - name: pg-password
        value: "$(yaml_str "$AZURE_POSTGRES_PASSWORD")"
  template:
    containers:
      - name: implied-moves
        image: ${IMAGE}
        command: ["python", "database/implied_moves_job.py"]
        resources:
          cpu: 0.5
          memory: 1Gi
        env:
          - {name: YAHOO_REQUESTS_PER_HOUR, value: "${YAHOO_REQUESTS_PER_HOUR}"}
          - {name: TICKZEN_RATE_LIMIT_BACKEND, value: "local"}
          - {name: TZ, value: "Asia/Kolkata"}
          - {name: AZURE_POSTGRES_HOST, value: "${PG_HOST}"}
          - {name: AZURE_POSTGRES_PORT, value: "${PG_PORT}"}
          - {name: AZURE_POSTGRES_DB, value: "${PG_DB}"}
          - {name: AZURE_POSTGRES_USER, value: "${PG_USER}"}
          - {name: AZURE_POSTGRES_SSLMODE, value: "require"}
          - {name: AZURE_POSTGRES_PASSWORD, secretRef: pg-password}
EOF

if az containerapp job show -g "$RESOURCE_GROUP" -n "$JOB_NAME" -o none 2>/dev/null; then
  echo "==> Updating job ${JOB_NAME}"
  az containerapp job update -g "$RESOURCE_GROUP" -n "$JOB_NAME" --yaml "$SPEC" -o none
else
  echo "==> Creating job ${JOB_NAME}"
  az containerapp job create -g "$RESOURCE_GROUP" -n "$JOB_NAME" --yaml "$SPEC" -o none
fi

cat <<EOF

Done: job ${JOB_NAME} in ${LOCATION}, image ${IMAGE}.
  Run now (market hours): az containerapp job start -g ${RESOURCE_GROUP} -n ${JOB_NAME}
  History:                az containerapp job execution list -g ${RESOURCE_GROUP} -n ${JOB_NAME} -o table
  Logs:                   az containerapp job logs show -g ${RESOURCE_GROUP} -n ${JOB_NAME} --container implied-moves --follow
EOF
