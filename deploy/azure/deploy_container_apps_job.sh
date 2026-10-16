#!/usr/bin/env bash
# Deploy ONE exporter worker as an Azure Container Apps scheduled job. Run it once per worker.
#
#   export AZURE_POSTGRES_PASSWORD=... FINNHUB_API_KEY=... FRED_API_KEY=...
#   WORKER_INDEX=0 WORKER_COUNT=6 LOCATION=centralindia deploy/azure/deploy_container_apps_job.sh
#   WORKER_INDEX=1 WORKER_COUNT=6 LOCATION=southindia   SKIP_BUILD=1 deploy/azure/deploy_container_apps_job.sh
#   ...
#
# What it creates (names derive from LOCATION / WORKER_INDEX; re-running updates in place):
#   * resource group ${RESOURCE_GROUP} and container registry ${ACR_NAME} (shared by all workers; the image
#     is built in Azure with `az acr build`, Docker is not needed - SKIP_BUILD=1 reuses the last image)
#   * per LOCATION: a Container Apps environment (its own outbound IP, so its own Yahoo budget) and a storage
#     account + file share mounted at /app/generated_data, so the monthly profile/financials/peer caches survive
#     between runs (a cold stock costs ~3x the Yahoo requests of a cached one)
#   * job tickzen-db-updater-${WORKER_INDEX}: starts every hour at :30 (16:30 New York = the start of the export
#     window, whatever the daylight-saving offset). A run exits at once while the US market is open, when
#     everything is current, or when this worker is already running. Otherwise it exports at most
#     STOCKS_PER_RUN stocks (its own part first, then stale stocks of other parts) within
#     RUN_TIME_BUDGET_MINUTES, then stops until the next hour.
#
# WORKER_COUNT must be the same on every worker. When you change it, redeploy every worker with the new value
# (SKIP_BUILD=1 is enough); stocks are re-split automatically.
#
# New-stock loader (JOB_MODE=new-stocks): job tickzen-db-new-stocks, every 15 minutes at any hour, gives stocks
# that were just added to monitored_tickers their first full load (at most 10 per run). Put it in a region no
# nightly worker uses, so it has its own outbound IP and Yahoo budget:
#   JOB_MODE=new-stocks LOCATION=southeastasia SKIP_BUILD=1 deploy/azure/deploy_container_apps_job.sh
#
# Prerequisites: `brew install azure-cli`, `az login`, `az extension add -n containerapp`.
set -euo pipefail

JOB_MODE="${JOB_MODE:-worker}"
if [[ "$JOB_MODE" == "new-stocks" ]]; then
  WORKER_INDEX="${WORKER_INDEX:-0}"
  WORKER_COUNT="${WORKER_COUNT:-1}"
  JOB_NAME="${JOB_NAME:-tickzen-db-new-stocks}"
  CRON_EXPRESSION="${CRON_EXPRESSION:-*/15 * * * *}"
  REPLICA_TIMEOUT_SECONDS="${REPLICA_TIMEOUT_SECONDS:-1800}"
  YAHOO_REQUESTS_PER_HOUR="${YAHOO_REQUESTS_PER_HOUR:-900}"
  STOCKS_PER_RUN="${STOCKS_PER_RUN:-10}"
  RUN_TIME_BUDGET_MINUTES="${RUN_TIME_BUDGET_MINUTES:-13}"
  EXTRA_ENV_YAML='          - {name: NEW_STOCKS_ONLY, value: "1"}
          - {name: WORKER_NAME, value: "azure-new-stocks"}'
fi
EXTRA_ENV_YAML="${EXTRA_ENV_YAML:-}"

: "${WORKER_INDEX:?set WORKER_INDEX (0-based)}"
: "${WORKER_COUNT:?set WORKER_COUNT (total workers, same on every worker)}"
: "${LOCATION:?set LOCATION (Azure region, e.g. centralindia)}"
: "${AZURE_POSTGRES_PASSWORD:?export AZURE_POSTGRES_PASSWORD first}"
: "${FINNHUB_API_KEY:?export FINNHUB_API_KEY first}"
: "${FRED_API_KEY:?export FRED_API_KEY first}"

RESOURCE_GROUP="${RESOURCE_GROUP:-tickzen-db-exporter-rg}"
RG_LOCATION="${RG_LOCATION:-centralindia}"
ACR_NAME="${ACR_NAME:-tickzendbexporter}"                 # globally unique, lowercase letters/digits only
IMAGE_REPO="tickzen-db-exporter"
ENV_NAME="${ENV_NAME:-tickzen-exporter-${LOCATION}}"
JOB_NAME="${JOB_NAME:-tickzen-db-updater-${WORKER_INDEX}}"
STORAGE_ACCOUNT="${STORAGE_ACCOUNT:-$(echo "tzcache${LOCATION}" | tr -cd 'a-z0-9' | cut -c1-24)}"
SHARE_NAME="generated-data"
CRON_EXPRESSION="${CRON_EXPRESSION:-30 * * * *}"           # hourly at :30, UTC; the runner decides whether to work
REPLICA_TIMEOUT_SECONDS="${REPLICA_TIMEOUT_SECONDS:-7200}"  # 2 h: a run stops itself after 55 min plus the stock in progress
YAHOO_REQUESTS_PER_HOUR="${YAHOO_REQUESTS_PER_HOUR:-1200}"
STOCKS_PER_RUN="${STOCKS_PER_RUN:-70}"
RUN_TIME_BUDGET_MINUTES="${RUN_TIME_BUDGET_MINUTES:-55}"
CPU="${CPU:-1.0}"
MEMORY="${MEMORY:-2Gi}"

PG_HOST="${AZURE_POSTGRES_HOST:-tickzen-stock-data-v2.postgres.database.azure.com}"
PG_DB="${AZURE_POSTGRES_DB:-tickzen-data}"
PG_USER="${AZURE_POSTGRES_USER:-jadaunkg}"
PG_PORT="${AZURE_POSTGRES_PORT:-5432}"

cd "$(dirname "$0")/../.."

echo "==> Resource providers"
for provider in Microsoft.App Microsoft.OperationalInsights Microsoft.ContainerRegistry Microsoft.Storage; do
  [[ "$(az provider show -n "$provider" --query registrationState -o tsv)" == "Registered" ]] \
    || az provider register -n "$provider" --wait -o none
done

echo "==> Resource group ${RESOURCE_GROUP}"
az group create -n "$RESOURCE_GROUP" -l "$RG_LOCATION" -o none

echo "==> Container registry ${ACR_NAME}"
az acr show -n "$ACR_NAME" -o none 2>/dev/null \
  || az acr create -g "$RESOURCE_GROUP" -n "$ACR_NAME" -l "$RG_LOCATION" --sku Basic --admin-enabled true -o none
az acr update -n "$ACR_NAME" --admin-enabled true -o none

if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
  TAG="$(az acr repository show-tags -n "$ACR_NAME" --repository "$IMAGE_REPO" --orderby time_desc --top 1 -o tsv)"
  echo "==> Reusing image tag ${TAG}"
else
  TAG="$(git rev-parse --short HEAD 2>/dev/null || date +%Y%m%d%H%M)$(git diff --quiet 2>/dev/null || echo "-dirty-$(date +%Y%m%d%H%M%S)")"
  echo "==> Building ${IMAGE_REPO}:${TAG} in Azure"
  az acr build -r "$ACR_NAME" -t "${IMAGE_REPO}:${TAG}" .
fi
IMAGE="${ACR_NAME}.azurecr.io/${IMAGE_REPO}:${TAG}"
ACR_PASSWORD="$(az acr credential show -n "$ACR_NAME" --query 'passwords[0].value' -o tsv)"

echo "==> Container Apps environment ${ENV_NAME} (${LOCATION})"
az containerapp env show -g "$RESOURCE_GROUP" -n "$ENV_NAME" -o none 2>/dev/null \
  || az containerapp env create -g "$RESOURCE_GROUP" -n "$ENV_NAME" -l "$LOCATION" -o none
ENV_ID="$(az containerapp env show -g "$RESOURCE_GROUP" -n "$ENV_NAME" --query id -o tsv)"

echo "==> Cache share ${STORAGE_ACCOUNT}/${SHARE_NAME}"
az storage account show -g "$RESOURCE_GROUP" -n "$STORAGE_ACCOUNT" -o none 2>/dev/null \
  || az storage account create -g "$RESOURCE_GROUP" -n "$STORAGE_ACCOUNT" -l "$LOCATION" \
       --sku Standard_LRS --kind StorageV2 --min-tls-version TLS1_2 -o none
STORAGE_KEY="$(az storage account keys list -g "$RESOURCE_GROUP" -n "$STORAGE_ACCOUNT" --query '[0].value' -o tsv)"
az storage share-rm show -g "$RESOURCE_GROUP" --storage-account "$STORAGE_ACCOUNT" -n "$SHARE_NAME" -o none 2>/dev/null \
  || az storage share-rm create -g "$RESOURCE_GROUP" --storage-account "$STORAGE_ACCOUNT" -n "$SHARE_NAME" --quota 20 -o none
az containerapp env storage set -g "$RESOURCE_GROUP" -n "$ENV_NAME" --storage-name exportercache \
  --azure-file-account-name "$STORAGE_ACCOUNT" --azure-file-account-key "$STORAGE_KEY" \
  --azure-file-share-name "$SHARE_NAME" --access-mode ReadWrite -o none

# Escape a value for a double-quoted YAML string (passwords may contain " or \).
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
      - name: finnhub-key
        value: "$(yaml_str "$FINNHUB_API_KEY")"
      - name: fred-key
        value: "$(yaml_str "$FRED_API_KEY")"
  template:
    containers:
      - name: exporter
        image: ${IMAGE}
        resources:
          cpu: ${CPU}
          memory: ${MEMORY}
        env:
          - {name: WORKER_INDEX, value: "${WORKER_INDEX}"}
          - {name: WORKER_COUNT, value: "${WORKER_COUNT}"}
          - {name: WORKER_REGION, value: "${LOCATION}"}
          - {name: IMAGE_TAG, value: "${TAG}"}
          - {name: YAHOO_REQUESTS_PER_HOUR, value: "${YAHOO_REQUESTS_PER_HOUR}"}
          - {name: STOCKS_PER_RUN, value: "${STOCKS_PER_RUN}"}
          - {name: RUN_TIME_BUDGET_MINUTES, value: "${RUN_TIME_BUDGET_MINUTES}"}
          - {name: STOP_AFTER_429, value: "3"}
${EXTRA_ENV_YAML}
          - {name: TICKZEN_RATE_LIMIT_BACKEND, value: "local"}
          - {name: TZ, value: "Asia/Kolkata"}
          - {name: AZURE_POSTGRES_HOST, value: "${PG_HOST}"}
          - {name: AZURE_POSTGRES_PORT, value: "${PG_PORT}"}
          - {name: AZURE_POSTGRES_DB, value: "${PG_DB}"}
          - {name: AZURE_POSTGRES_USER, value: "${PG_USER}"}
          - {name: AZURE_POSTGRES_SSLMODE, value: "require"}
          - {name: AZURE_POSTGRES_PASSWORD, secretRef: pg-password}
          - {name: FINNHUB_API_KEY, secretRef: finnhub-key}
          - {name: FRED_API_KEY, secretRef: fred-key}
        volumeMounts:
          - volumeName: cache
            mountPath: /app/generated_data
    volumes:
      - name: cache
        storageType: AzureFile
        storageName: exportercache
EOF

if az containerapp job show -g "$RESOURCE_GROUP" -n "$JOB_NAME" -o none 2>/dev/null; then
  echo "==> Updating job ${JOB_NAME}"
  az containerapp job update -g "$RESOURCE_GROUP" -n "$JOB_NAME" --yaml "$SPEC" -o none
else
  echo "==> Creating job ${JOB_NAME}"
  az containerapp job create -g "$RESOURCE_GROUP" -n "$JOB_NAME" --yaml "$SPEC" -o none
fi

PG_SERVER="${PG_HOST%%.*}"
PG_RG="$(az postgres flexible-server list --query "[?name=='${PG_SERVER}'].resourceGroup | [0]" -o tsv 2>/dev/null || true)"
if [[ -n "$PG_RG" ]] && [[ -z "$(az postgres flexible-server firewall-rule list -g "$PG_RG" --server-name "$PG_SERVER" \
      --query "[?startIpAddress=='0.0.0.0' && endIpAddress=='0.0.0.0'].name | [0]" -o tsv)" ]]; then
  echo
  echo "!! Postgres server ${PG_SERVER} does not admit Azure services; Container Apps jobs have no fixed IP. Run:"
  echo "   az postgres flexible-server firewall-rule create -g ${PG_RG} --server-name ${PG_SERVER} \\"
  echo "     --rule-name AllowAzureServices --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0"
fi

cat <<EOF

Done: ${JOB_MODE} job ${JOB_NAME} (worker $((WORKER_INDEX + 1)) of ${WORKER_COUNT}) in ${LOCATION}, image ${IMAGE}.
  Run now:          az containerapp job start -g ${RESOURCE_GROUP} -n ${JOB_NAME}
  History:          az containerapp job execution list -g ${RESOURCE_GROUP} -n ${JOB_NAME} -o table
  Logs:             az containerapp job logs show -g ${RESOURCE_GROUP} -n ${JOB_NAME} --container exporter --follow
EOF
