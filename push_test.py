import json
from google.cloud import bigquery
from google.cloud import pubsub_v1

project_id = ""

def process_and_publish_csv_status():
    """
    Queries BigQuery for CSV analysis status with null status, publishes to Pub/Sub,
    and updates the status in BigQuery using google-cloud-bigquery library.

    Returns:
        int: The number of successfully processed and updated rows.
    """
    topic_id = "midata_topic"
    
    # Initialize Clients
    publisher = pubsub_v1.PublisherClient()
    bq_client = bigquery.Client(project=project_id)
    
    topic_path = publisher.topic_path(project_id, topic_id)

    # 1. Select Query
    sql = f"""
    SELECT uri, content_type
    FROM `{project_id}.csv_parse_ds.csv_analysis_status`
    WHERE status IS NULL
    LIMIT 10
    """

    try:
        # Run the SELECT query
        query_job = bq_client.query(sql)
        rows = query_job.result()  # Waits for query to complete and returns iterator
    except Exception as e:
        print(f"Error fetching data from BigQuery: {e}")
        return 0

    success_count = 0

    for row in rows:
        uri = row['uri']
        try:
            # Convert BigQuery Row to Dict -> JSON String
            # row behaves like a dictionary
            message_data = dict(row) 
            message_json = json.dumps(message_data)
            message_bytes = message_json.encode("utf-8")

            # 2. Publish to Pub/Sub
            future = publisher.publish(topic_path, message_bytes)
            future.result()  # Wait for the publish operation to complete

            # 3. Update status in BigQuery
            # Use parameterized query for safety (handling special chars in URI)
            update_sql = f"""
            UPDATE `{project_id}.csv_parse_ds.csv_analysis_status`
            SET status = 'new'
            WHERE uri = @uri
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("uri", "STRING", uri),
                ]
            )
            
            update_job = bq_client.query(update_sql, job_config=job_config)
            update_job.result()  # Wait for the update to complete

            success_count += 1

        except Exception as e:
            print(f"Error processing row {uri}: {e}")
            # Optionally continue to next row or break depending on requirements
            continue

    return success_count

if __name__ == "__main__":
    processed_count = process_and_publish_csv_status()
    print(f"Successfully processed and updated {processed_count} rows.")
