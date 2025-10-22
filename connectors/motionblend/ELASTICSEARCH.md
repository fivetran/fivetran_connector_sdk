# Elasticsearch Integration for MotionBlend Fivetran Connector

## Overview

This guide explains how to integrate Elasticsearch with the MotionBlend connector to enable fast, full-text search and analytics on motion blend quality metrics.

## Architecture

```
GCS (BVH files) 
  → Fivetran Connector → BigQuery (RAW tables)
    → dbt Transformations → BigQuery (MARTS tables)
      → Batch Exporter → Elasticsearch (searchable index)
        → Search API → Client applications
```

## Why Elasticsearch?

1. **Fast Search**: Sub-second queries across millions of motion blends
2. **Rich Filtering**: Complex queries combining quality scores, diversity metrics, and transition smoothness
3. **Aggregations**: Real-time analytics on motion quality distributions
4. **Scoring**: Custom relevance scoring for motion recommendation
5. **Scalability**: Horizontal scaling for large animation studios

## Index Schema

### Core Fields

```json
{
  "blend_id": "keyword",           // Unique identifier
  "left_motion_id": "keyword",     // Source motion (left)
  "right_motion_id": "keyword",    // Source motion (right)
  "blend_ratio": "float",          // Blend weight (0-1)
  "transition_start_frame": "integer",
  "transition_end_frame": "integer",
  "method": "keyword"              // Blend algorithm (e.g., "snn")
}
```

### Quality Metrics Fields

```json
{
  // Distribution metrics
  "fid": "float",                  // Fréchet Inception Distance (lower is better)
  "coverage": "float",             // Coverage metric (0-1, higher is better)
  
  // Diversity metrics
  "global_diversity": "float",     // Variance across entire sequence
  "local_diversity": "float",      // Average variance in windows
  "inter_diversity": "float",      // Variance between joints (spatial)
  "intra_diversity": "float",      // Variance within joints (temporal)
  
  // L2 Velocity metrics (smoothness)
  "l2_velocity_mean": "float",
  "l2_velocity_std": "float",
  "l2_velocity_max": "float",
  "l2_velocity_transition": "float",
  
  // L2 Acceleration metrics
  "l2_acceleration_mean": "float",
  "l2_acceleration_std": "float",
  "l2_acceleration_max": "float",
  "l2_acceleration_transition": "float",
  
  // Transition quality
  "transition_smoothness": "float",
  "velocity_ratio": "float",
  "acceleration_ratio": "float",
  
  // Overall assessment
  "quality_score": "float",        // 0-1 composite score
  "quality_category": "keyword"    // excellent/good/acceptable/poor
}
```

### Issue Flags

```json
{
  "has_velocity_spike": "boolean",       // Sudden speed changes detected
  "has_rough_transition": "boolean",     // Transition region discontinuity
  "has_distribution_mismatch": "boolean" // FID above threshold
}
```

## Setup

### 1. Install Elasticsearch

**Docker (Development):**
```bash
docker run -d \
  --name elasticsearch \
  -p 9200:9200 \
  -p 9300:9300 \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  elasticsearch:8.11.0
```

**Cloud (Production):**
- Elastic Cloud: https://cloud.elastic.co
- AWS Elasticsearch/OpenSearch
- Google Cloud Elasticsearch

### 2. Configure Environment

```bash
export ELASTICSEARCH_URL="http://localhost:9200"
export ELASTICSEARCH_API_KEY="your-api-key"  # Optional for auth
export GCP_PROJECT="motionblend-ai"
```

### 3. Install Dependencies

```bash
pip install elasticsearch google-cloud-bigquery
```

### 4. Run Initial Export

```bash
python exporter/bigquery_to_elastic.py \
  --bq-project motionblend-ai \
  --bq-dataset RAW_DEV_marts \
  --bq-table mart_blend_snn_metrics \
  --es-url http://localhost:9200 \
  --es-index mb_blends_v1 \
  --batch-size 100
```

## Search Examples

### 1. Find High-Quality Blends

```bash
curl -X GET "localhost:9200/mb_blends_v1/_search" -H 'Content-Type: application/json' -d'
{
  "query": {
    "bool": {
      "must": [
        {"range": {"quality_score": {"gte": 0.8}}},
        {"term": {"quality_category": "excellent"}}
      ]
    }
  },
  "sort": [{"quality_score": {"order": "desc"}}],
  "size": 50
}
'
```

### 2. Smooth Transitions Only

```bash
curl -X GET "localhost:9200/mb_blends_v1/_search" -H 'Content-Type: application/json' -d'
{
  "query": {
    "bool": {
      "must": [
        {"range": {"transition_smoothness": {"gte": 0.85}}},
        {"term": {"has_rough_transition": false}}
      ],
      "filter": [
        {"range": {"l2_velocity_max": {"lte": 2.0}}}
      ]
    }
  }
}
'
```

### 3. High Diversity Blends

```bash
curl -X GET "localhost:9200/mb_blends_v1/_search" -H 'Content-Type: application/json' -d'
{
  "query": {
    "bool": {
      "must": [
        {"range": {"global_diversity": {"gte": 1.5}}},
        {"range": {"coverage": {"gte": 0.9}}}
      ]
    }
  },
  "sort": [{"global_diversity": {"order": "desc"}}]
}
'
```

### 4. Find Similar Blends (by Motion ID)

```bash
curl -X GET "localhost:9200/mb_blends_v1/_search" -H 'Content-Type: application/json' -d'
{
  "query": {
    "bool": {
      "should": [
        {"term": {"left_motion_id": "abc123"}},
        {"term": {"right_motion_id": "abc123"}}
      ],
      "minimum_should_match": 1
    }
  }
}
'
```

### 5. Quality Distribution Aggregation

```bash
curl -X GET "localhost:9200/mb_blends_v1/_search" -H 'Content-Type: application/json' -d'
{
  "size": 0,
  "aggs": {
    "quality_categories": {
      "terms": {"field": "quality_category"}
    },
    "avg_quality": {
      "avg": {"field": "quality_score"}
    },
    "quality_histogram": {
      "histogram": {
        "field": "quality_score",
        "interval": 0.1
      }
    }
  }
}
'
```

### 6. Issue Detection

```bash
curl -X GET "localhost:9200/mb_blends_v1/_search" -H 'Content-Type: application/json' -d'
{
  "query": {
    "bool": {
      "should": [
        {"term": {"has_velocity_spike": true}},
        {"term": {"has_rough_transition": true}},
        {"term": {"has_distribution_mismatch": true}}
      ],
      "minimum_should_match": 1
    }
  },
  "aggs": {
    "issue_types": {
      "filters": {
        "filters": {
          "velocity_spikes": {"term": {"has_velocity_spike": true}},
          "rough_transitions": {"term": {"has_rough_transition": true}},
          "distribution_mismatches": {"term": {"has_distribution_mismatch": true}}
        }
      }
    }
  }
}
'
```

## Python Search Client

```python
from elasticsearch import Elasticsearch

# Initialize client
es = Elasticsearch(["http://localhost:9200"])

# Search for excellent blends
response = es.search(
    index="mb_blends_v1",
    body={
        "query": {
            "bool": {
                "must": [
                    {"range": {"quality_score": {"gte": 0.8}}},
                    {"term": {"quality_category": "excellent"}}
                ]
            }
        },
        "sort": [{"quality_score": {"order": "desc"}}],
        "size": 50
    }
)

# Process results
for hit in response['hits']['hits']:
    blend = hit['_source']
    print(f"Blend {blend['blend_id']}: score={blend['quality_score']:.3f}")
```

## Incremental Updates

### Scheduled Batch Export (Cloud Run Job)

```yaml
# cloud-run-job.yaml
apiVersion: run.googleapis.com/v1
kind: Job
metadata:
  name: bigquery-to-elasticsearch
spec:
  template:
    spec:
      containers:
      - image: gcr.io/motionblend-ai/exporter:latest
        command: ["python", "exporter/bigquery_to_elastic.py"]
        args:
          - "--bq-project=motionblend-ai"
          - "--bq-dataset=RAW_PROD_marts"
          - "--bq-table=mart_blend_snn_metrics"
          - "--es-url=$(ELASTICSEARCH_URL)"
          - "--es-api-key=$(ELASTICSEARCH_API_KEY)"
        env:
        - name: ELASTICSEARCH_URL
          valueFrom:
            secretKeyRef:
              name: elasticsearch-config
              key: url
        - name: ELASTICSEARCH_API_KEY
          valueFrom:
            secretKeyRef:
              name: elasticsearch-config
              key: api_key
```

Deploy with Cloud Scheduler:
```bash
gcloud scheduler jobs create http bigquery-to-es-sync \
  --schedule="0 */6 * * *" \
  --uri="https://run.googleapis.com/apis/run.googleapis.com/v1/namespaces/motionblend-ai/jobs/bigquery-to-elasticsearch:run" \
  --http-method=POST \
  --oauth-service-account-email=cloud-scheduler@motionblend-ai.iam.gserviceaccount.com
```

### Incremental Sync Query

For large datasets, export only recent changes:

```python
query = f"""
    SELECT *
    FROM `{project}.{dataset}.{table}`
    WHERE computed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
"""
```

## Performance Tuning

### Index Settings

```json
{
  "settings": {
    "number_of_shards": 3,        // Scale with data size
    "number_of_replicas": 1,      // For high availability
    "refresh_interval": "30s",    // Reduce for faster indexing
    "index.max_result_window": 10000
  }
}
```

### Bulk Indexing

- **Batch size**: 100-500 records per bulk request
- **Parallel workers**: 4-8 for large exports
- **Refresh interval**: Increase to 30s-60s during bulk loads

### Query Optimization

1. **Use filters over queries** for exact matches (no scoring)
2. **Limit fields** with `_source: ["field1", "field2"]`
3. **Paginate** with `from` and `size` (max 10,000 results)
4. **Use scroll API** for deep pagination
5. **Cache common aggregations** with `request_cache: true`

## Monitoring

### Health Check

```bash
curl -X GET "localhost:9200/_cluster/health?pretty"
curl -X GET "localhost:9200/mb_blends_v1/_stats?pretty"
```

### Index Size

```bash
curl -X GET "localhost:9200/_cat/indices/mb_blends_v1?v"
```

### Query Performance

```bash
curl -X GET "localhost:9200/mb_blends_v1/_search" -H 'Content-Type: application/json' -d'
{
  "profile": true,
  "query": {...}
}
'
```

## Integration with Fivetran

### Complete Pipeline

1. **Fivetran Connector** syncs GCS → BigQuery RAW tables
2. **dbt** transforms RAW → MARTS with quality metrics
3. **Batch Exporter** (this tool) exports MARTS → Elasticsearch
4. **Search API** queries Elasticsearch for fast lookups

### Automation

```bash
# Option 1: Makefile target
make elasticsearch-export

# Option 2: Cloud Scheduler (every 6 hours)
gcloud scheduler jobs create ...

# Option 3: dbt post-hook (after marts refresh)
# In dbt_project.yml:
on-run-end:
  - "{{ run_elasticsearch_export() }}"
```

## Troubleshooting

### Connection Refused
- Check Elasticsearch is running: `curl localhost:9200`
- Verify firewall rules allow port 9200

### Index Not Found
- Create index manually or let exporter create it automatically
- Check index name matches configuration

### Slow Queries
- Add indices on frequently filtered fields
- Use aggregations sparingly
- Enable query caching

### Memory Issues
- Increase JVM heap: `-Xms2g -Xmx2g`
- Reduce batch size in exporter
- Monitor with `GET /_nodes/stats`

## Security

### Production Checklist

- [ ] Enable authentication (X-Pack or API keys)
- [ ] Use HTTPS for all connections
- [ ] Restrict network access with firewall rules
- [ ] Rotate API keys regularly
- [ ] Enable audit logging
- [ ] Encrypt data at rest
- [ ] Use VPC peering for Elastic Cloud

### API Key Authentication

```bash
# Create API key in Elasticsearch
curl -X POST "localhost:9200/_security/api_key" -H 'Content-Type: application/json' -d'
{
  "name": "motionblend-exporter",
  "role_descriptors": {
    "motionblend_writer": {
      "cluster": ["monitor"],
      "indices": [{
        "names": ["mb_blends_*"],
        "privileges": ["write", "create_index"]
      }]
    }
  }
}
'

# Use in exporter
export ELASTICSEARCH_API_KEY="<api_key_from_response>"
```

## Cost Optimization

### Elastic Cloud Pricing

- **Storage**: ~$0.30/GB/month
- **Compute**: ~$0.15/hour for t3.medium equivalent
- **Data transfer**: Free within same cloud region

### Best Practices

1. Use **lifecycle policies** to delete old indices
2. **Compress** large text fields
3. **Disable** `_source` for large documents if not needed
4. Use **frozen indices** for archived data
5. **Deduplicate** before indexing (use `blend_id` as document ID)

## References

- [Elasticsearch Python Client](https://elasticsearch-py.readthedocs.io/)
- [Query DSL Reference](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html)
- [Bulk API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html)
- [Index Lifecycle Management](https://www.elastic.co/guide/en/elasticsearch/reference/current/index-lifecycle-management.html)

## Support

For issues or questions:
- Open an issue in the MotionBlendAI repository
- Check Elasticsearch logs: `docker logs elasticsearch`
- Review BigQuery export logs in Cloud Logging
