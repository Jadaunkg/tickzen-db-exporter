#!/usr/bin/env bash
# Health check for the exporter workers: per worker and day, how many stocks were exported, Yahoo requests per
# stock, and HTTP 429 responses (from the run summaries in Log Analytics), plus the latest executions.
#
#   deploy/azure/check_workers.sh            # last 7 days
#   DAYS=1 deploy/azure/check_workers.sh
#
# Healthy: http_429 stays at 0 (it counts every "Yahoo answered HTTP 429" warning, even from runs that crashed). Any 429 means that worker's region is being throttled; lower
# YAHOO_REQUESTS_PER_HOUR for it or move it to another region before adding stocks.
set -euo pipefail
export PYTHONWARNINGS=ignore

RESOURCE_GROUP="${RESOURCE_GROUP:-tickzen-db-exporter-rg}"
DAYS="${DAYS:-7}"

QUERY="ContainerAppConsoleLogs_CL
| where TimeGenerated > ago(${DAYS}d)
| where Log_s has 'BATCH EXPORT RUN COMPLETE' or Log_s has 'Processed: ' or Log_s has 'Yahoo requests: ' or Log_s has 'answered HTTP 429'
| extend job = ContainerJobName_s, day = format_datetime(TimeGenerated, 'yyyy-MM-dd')
| extend processed = toint(extract(@'Processed: (\\d+)', 1, Log_s)),
         requests = toint(extract(@'Yahoo requests: (\\d+) \\(', 1, Log_s)),
         live429 = iif(Log_s has 'answered HTTP 429', 1, 0)
| summarize stocks = sum(processed), yahoo_requests = sum(requests), throttle_warnings = sum(live429),
            runs = countif(Log_s has 'BATCH EXPORT RUN COMPLETE') by job, day
| extend per_stock = round(todouble(yahoo_requests) / iif(stocks == 0, 1, stocks), 1)
| order by day desc, job asc"

for ws in $(az monitor log-analytics workspace list -g "$RESOURCE_GROUP" --query "[].customerId" -o tsv); do
  echo "== Log Analytics workspace ${ws}"
  az monitor log-analytics query -w "$ws" --analytics-query "$QUERY" \
    --query "[].{day:day, job:job, runs:runs, stocks:stocks, yahoo_requests:yahoo_requests, per_stock:per_stock, http_429:throttle_warnings}" -o table
done

echo
echo "== Latest executions"
for job in $(az containerapp job list -g "$RESOURCE_GROUP" --query "[].name" -o tsv); do
  az containerapp job execution list -g "$RESOURCE_GROUP" -n "$job" \
    --query "sort_by([], &properties.startTime)[-3:].{job:'${job}', start:properties.startTime, status:properties.status}" -o table
done
