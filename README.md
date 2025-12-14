# Gemini Bulk Call

BigQuery + + pub/sub Cloud Run 
This sample shows how to create a service that call the gemini for the bulk files. 

## Local Run 

Environment Variable: .env 
Copy the .env.example to .env and set the variables. 

```bash
uv run gunicorn --bind :8080 --workers 1 --threads 8 --timeout 0 main:app
```


## Build

```
docker build --tag pubsub-tutorial:python .
```

## Run Locally

```
docker run --rm -p 9090:8080 -e PORT=8080 pubsub-tutorial:python
```

## Test

```
pytest
```

_Note: you may need to install `pytest` using `pip install pytest`._

## Deploy

```
# Set an environment variable with your GCP Project ID
export GOOGLE_CLOUD_PROJECT=<PROJECT_ID>

# Submit a build using Google Cloud Build
gcloud builds submit --tag gcr.io/${GOOGLE_CLOUD_PROJECT}/pubsub-tutorial

# Deploy to Cloud Run
gcloud run deploy pubsub-tutorial --image gcr.io/${GOOGLE_CLOUD_PROJECT}/pubsub-tutorial
```
