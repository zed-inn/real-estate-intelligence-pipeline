#!/bin/bash
set -uo pipefail

PROJ_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="$PROJ_DIR/results/$TIMESTAMP"
mkdir -p "$RESULTS_DIR"

DC="sudo docker compose"
NETWORK="real-estate-pipeline_default"
PROM="http://localhost:9090/api/v1/query"

echo "============================================"
echo " Real Estate Load Test Runner"
echo " Results -> $RESULTS_DIR"
echo "============================================"

echo "[1/4] Ensuring services are up and resetting database..."
cd $PROJ_DIR && make docker-up
sleep 5

echo "Deep cleaning Kafka pipeline (Hard Reset)..."
$DC stop engine gateway kafka
$DC rm -f kafka
sudo docker volume rm real-estate-pipeline_kafka_data 2>/dev/null || true
$DC up -d kafka engine gateway
echo "Waiting for Kafka to re-initialize..."
sleep 15 
$DC exec -T db psql -U db_admin -d intelligence_db -c "TRUNCATE real_estate_listings CASCADE; TRUNCATE event_relay_queue CASCADE;"

echo "[2/4] Firing K6 (10,000 requests, 50 VUs)..."
sudo docker run --rm -i --network "$NETWORK" \
  grafana/k6 run - < "$PROJ_DIR/scripts/load_test.js" \
  2>&1 | tee "$RESULTS_DIR/k6_output.txt"

echo "[3/4] Waiting for Kafka queues to drain..."
for i in $(seq 1 600); do
  LAG=$(curl -s "http://localhost:8080/api/consumer-groups" | python3 -c "import sys,json; data=json.load(sys.stdin); print(sum(topic.get('summedLag',0) for group in data.get('consumerGroups',[]) for topic in group.get('topicOffsets',[])))" 2>/dev/null || echo "?")
  
  if [[ "$LAG" == "0" ]]; then
    echo "[*] Queues drained!"
    break
  fi
  echo "  Waiting for Python Engine to process embeddings... (Lag: $LAG)"
  sleep 3
done

echo "[4/4] Extracting Comprehensive Metrics and Validating DB..."

# ----------------------------------------------------------
# Gateway (Node.js/Fastify) Metrics
# ----------------------------------------------------------
function fetch_prom() {
  local query="$1"
  local outfile="$2"
  curl -sG --data-urlencode "query=$query" "$PROM" | tee "$RESULTS_DIR/$outfile" > /dev/null
}

fetch_prom "nodejs_heap_size_used_bytes" "prom_node_heap_used.json"
fetch_prom "nodejs_heap_size_total_bytes" "prom_node_heap_total.json"
fetch_prom "nodejs_external_memory_bytes" "prom_node_external_mem.json"
fetch_prom "nodejs_eventloop_lag_max_seconds" "prom_node_evloop_lag_max.json"
fetch_prom "nodejs_eventloop_lag_p99_seconds" "prom_node_evloop_lag_p99.json"
fetch_prom "nodejs_eventloop_lag_mean_seconds" "prom_node_evloop_lag_mean.json"
fetch_prom "nodejs_active_handles" "prom_node_active_handles.json"
fetch_prom "nodejs_active_requests_total" "prom_node_active_requests.json"
fetch_prom "nodejs_gc_duration_seconds_sum" "prom_node_gc.json"
fetch_prom "http_request_duration_seconds_count" "prom_http_req_count.json"
fetch_prom "http_request_duration_seconds_sum" "prom_http_req_duration_sum.json"
fetch_prom "http_request_summary_seconds" "prom_http_req_summary.json"
fetch_prom 'process_cpu_user_seconds_total{job="gateway"}' "prom_node_cpu_user.json"
fetch_prom 'process_cpu_system_seconds_total{job="gateway"}' "prom_node_cpu_system.json"
fetch_prom 'process_resident_memory_bytes{job="gateway"}' "prom_node_rss_memory.json"
fetch_prom 'process_virtual_memory_bytes{job="gateway"}' "prom_node_vms_memory.json"
fetch_prom 'process_open_fds{job="gateway"}' "prom_node_open_fds.json"
fetch_prom 'process_max_fds{job="gateway"}' "prom_node_max_fds.json"
fetch_prom "nodejs_version_info" "prom_node_version.json"
fetch_prom "nodejs_heap_space_size_total_bytes" "prom_node_heap_space_total.json"
fetch_prom "nodejs_heap_space_size_used_bytes" "prom_node_heap_space_used.json"
fetch_prom "nodejs_heap_space_size_available_bytes" "prom_node_heap_space_avail.json"
fetch_prom 'process_start_time_seconds{job="gateway"}' "prom_node_start_time.json"
fetch_prom "http_request_duration_seconds_bucket" "prom_http_req_duration_bucket.json"

# ----------------------------------------------------------
# Engine (Python) Metrics
# ----------------------------------------------------------
fetch_prom "engine_embeddings_total" "prom_python_embeddings.json"
fetch_prom "engine_embedding_processing_seconds_sum" "prom_python_embedding_duration_sum.json"
fetch_prom "engine_embedding_processing_seconds_count" "prom_python_embedding_duration_count.json"
fetch_prom 'process_resident_memory_bytes{job="engine"}' "prom_python_rss_memory.json"
fetch_prom 'process_cpu_seconds_total{job="engine"}' "prom_python_cpu_total.json"
fetch_prom "python_gc_objects_collected_total" "prom_python_gc.json"
fetch_prom 'process_virtual_memory_bytes{job="engine"}' "prom_python_vms_memory.json"
fetch_prom 'process_open_fds{job="engine"}' "prom_python_open_fds.json"
fetch_prom 'process_max_fds{job="engine"}' "prom_python_max_fds.json"
fetch_prom "python_gc_collections_total" "prom_python_gc_collections.json"
fetch_prom "python_gc_objects_uncollectable_total" "prom_python_gc_uncollectable.json"
fetch_prom "python_info" "prom_python_info.json"
fetch_prom 'process_start_time_seconds{job="engine"}' "prom_python_start_time.json"

# ----------------------------------------------------------
# Kafka Core Metrics (via Redpanda Console API)
# ----------------------------------------------------------
echo "Parsing specific Kafka metrics directly from Redpanda Console API..."

# 1. Total Brokers
curl -s "http://localhost:8080/api/cluster" | python3 -c "import sys,json; data=json.load(sys.stdin); print(len(data.get('brokers', [])))" 2>/dev/null | tee "$RESULTS_DIR/prom_kafka_brokers_count.json" > /dev/null
# 2. Total Partitions
curl -s "http://localhost:8080/api/topics" | python3 -c "import sys,json; data=json.load(sys.stdin); print(sum(t.get('partitionCount', 0) for t in data.get('topics', [])))" 2>/dev/null | tee "$RESULTS_DIR/prom_kafka_partitions.json" > /dev/null
# 3. Total Bytes In (Log Size)
curl -s "http://localhost:8080/api/topics" | python3 -c "import sys,json; data=json.load(sys.stdin); print(sum(t.get('logDirSummary', {}).get('totalSizeBytes', 0) for t in data.get('topics', [])))" 2>/dev/null | tee "$RESULTS_DIR/prom_kafka_in_bytes.json" > /dev/null
# 4. Consumer Group Lag
curl -s "http://localhost:8080/api/consumer-groups" | python3 -c "import sys,json; data=json.load(sys.stdin); print(sum(topic.get('summedLag',0) for group in data.get('consumerGroups',[]) for topic in group.get('topicOffsets',[])))" 2>/dev/null | tee "$RESULTS_DIR/prom_kafka_consumer_lag.json" > /dev/null
# 5. Consumer Group Offsets
curl -s "http://localhost:8080/api/consumer-groups" | python3 -c "import sys,json; data=json.load(sys.stdin); print(sum(p.get('groupOffset',0) for group in data.get('consumerGroups',[]) for topic in group.get('topicOffsets',[]) for p in topic.get('partitionOffsets',[])))" 2>/dev/null | tee "$RESULTS_DIR/prom_kafka_cg_offsets.json" > /dev/null
# 6. Topic Partition Offsets (High Water Mark)
curl -s "http://localhost:8080/api/consumer-groups" | python3 -c "import sys,json; data=json.load(sys.stdin); print(sum(p.get('highWaterMark',0) for group in data.get('consumerGroups',[]) for topic in group.get('topicOffsets',[]) for p in topic.get('partitionOffsets',[])))" 2>/dev/null | tee "$RESULTS_DIR/prom_kafka_topic_offsets.json" > /dev/null
# 7. Total Consumer Groups
curl -s "http://localhost:8080/api/consumer-groups" | python3 -c "import sys,json; data=json.load(sys.stdin); print(len(data.get('consumerGroups',[])))" 2>/dev/null | tee "$RESULTS_DIR/prom_kafka_cg_count.json" > /dev/null
# 8. Active Consumer Members (Connected Clients)
curl -s "http://localhost:8080/api/consumer-groups" | python3 -c "import sys,json; data=json.load(sys.stdin); print(sum(len(group.get('members',[])) for group in data.get('consumerGroups',[])))" 2>/dev/null | tee "$RESULTS_DIR/prom_kafka_active_consumers.json" > /dev/null
# 9. Dead/Empty Consumer Groups
curl -s "http://localhost:8080/api/consumer-groups" | python3 -c "import sys,json; data=json.load(sys.stdin); print(sum(1 for group in data.get('consumerGroups',[]) if group.get('state', '').lower() == 'empty'))" 2>/dev/null | tee "$RESULTS_DIR/prom_kafka_cg_empty.json" > /dev/null

# ----------------------------------------------------------
# DB Integrity
# ----------------------------------------------------------
echo "=== DB INTEGRITY CHECKS ===" > "$RESULTS_DIR/db_integrity.txt"
$DC exec -T db psql -U db_admin -d intelligence_db -c "SELECT COUNT(*) as total_records FROM real_estate_listings;" | tee -a "$RESULTS_DIR/db_integrity.txt"
$DC exec -T db psql -U db_admin -d intelligence_db -c "SELECT COUNT(*) as embedded_records FROM real_estate_listings WHERE embedding IS NOT NULL;" | tee -a "$RESULTS_DIR/db_integrity.txt"
$DC exec -T db psql -U db_admin -d intelligence_db -c "SELECT property_type, COUNT(*) FROM real_estate_listings GROUP BY property_type;" | tee -a "$RESULTS_DIR/db_integrity.txt"

echo "Load Test Complete. Results in $RESULTS_DIR."

# Auto-generate the comprehensive metrics.md report
bash "$PROJ_DIR/scripts/generate_metrics_md.sh"
