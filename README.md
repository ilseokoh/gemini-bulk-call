# Gemini Bulk Call

BigQuery + pub/sub + Cloud Run 
This sample shows how to create a service that call the gemini for the bulk files. 

## Create Pub/Sub, Artifact Repo,  

Create Artifacts Repository
```
gcloud artifacts repositories create bulk-data-repo \
    --repository-format=docker \
    --location=asia-northeast3
```

Create Pub/Sub Topic
```
gcloud pubsub topics create bulkdata-topic
```



## Local Run 

Environment Variable: .env 
Copy the .env.example to .env and set the variables. 

uv sync 
```
uv sync
```

Local run the http server 
```bash
uv run gunicorn --bind :8080 --workers 1 --threads 8 --timeout 0 main:app
```

Send Test Message 
```
curl -X POST   -H "Content-Type: application/json"   http://localhost:8080   -d '{"message": {"data": "{BASE64 encoded text}", "messageId": "17360707724236700", "message_id": "17360707724236700", "publishTime": "2025-12-13T04:20:45.343Z", "publish_time": "2025-12-13T04:20:45.343Z"},"subscription": "projects/pjt-lges-midata/subscriptions/cloud-run-sub"}'
```

## Build

Artifacts Repo Auth
```
gcloud auth configure-docker asia-northeast3-docker.pkg.dev
```

Build Image
```
docker build -t gemini-analysis-pubsub:v9 .
docker tag gemini-analysis-pubsub:v9 asia-northeast3-docker.pkg.dev/kevin-ai-playground/bulk-data-repo/gemini-analysis-pubsub:v9
docker push asia-northeast3-docker.pkg.dev/kevin-ai-playground/bulk-data-repo/gemini-analysis-pubsub:v9
```

## Deploy to Cloud Run

gcloud Auth
```
gcloud auth login
gcloud config set project PROJECT_ID
```

Deploy to Cloud Run with VCP egress
```
gcloud run deploy gemini-analysis-pubsub --image asia-northeast3-docker.pkg.dev/kevin-ai-playground/bulk-data-repo/gemini-analysis-pubsub:v9   --no-allow-unauthenticated  --project kevin-ai-playground  --region=asia-northeast3 --env-vars-file=.env --vpc-egress=all-traffic --network-tags=bulkdata-run --network=kevin-vpc --subnet=kevin-vpc --memory 32Gi
```

The result looks like this: 
```
Done.                                                                                                                                                                                                                                  
Service [gemini-analysis-pubsub] revision [gemini-analysis-pubsub-00001-1fr] has been deployed and is serving 100 percent of traffic.
Service URL: https://gemini-analysis-pubsub-83447119683.asia-northeast3.run.app
```

## Integrate Pub/Sub and Cloud Run 

Create Service Account 
```
gcloud iam service-accounts create cloud-run-pubsub-invoker \
    --display-name "Cloud Run Pub/Sub Invoker"
```


Give the invoker service account permission to invoke your Cloud Run
```
gcloud run services add-iam-policy-binding gemini-analysis-pubsub \
--member=serviceAccount:cloud-run-pubsub-invoker@{PROJECT_ID}.iam.gserviceaccount.com \
--role=roles/run.invoker
```

Create a Pub/Sub subscription with the service accoun
```
gcloud pubsub subscriptions create bulkdata-sub --topic bulkdata-topic \
--ack-deadline=600 \
--push-endpoint={CLOUD_RUN_SERVICE_URL}/ \
--push-auth-service-account=cloud-run-pubsub-invoker@{PROJECT_ID}.iam.gserviceaccount.com
```


Allow Pub/Sub to create authentication tokens in your project
```
gcloud projects add-iam-policy-binding {PROJECT_NUMBER} \
   --member=serviceAccount:service-{PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com \
   --role=roles/iam.serviceAccountTokenCreator
```

## Log 

resource.type="cloud_run_revision"
resource.labels.revision_name="gemini-analysis-pubsub-00002-8dd"
resource.labels.service_name="gemini-analysis-pubsub"