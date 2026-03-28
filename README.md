# F1 Analytics: Data-Engineering-Zoomcamp-26-final-project

## Brief description

N/A

## Prerequisities to run

1) GCP account
- with enabled project
- service account for terraform (Storage and BigQuery admin)
- service account key for this account
    - will likely encounter issue "Service account key creation is disabled"
    - open ```GCP console/terminal``` --> ```gcloud resource-manager org-policies disable-enforce iam.disableServiceAccountKeyCreation --project=PROJECT_NAME``` --> wait 10-15 minutes --> will be enabled --> save as e.g. ```service_account.json```

1a) google cloud SDK
- ...

2) terraform installed
- run to init terraform:
```bash
terraform init
```
- run to see what the terraform is about to create:
```bash
terraform plan
```
- run to let terraform actually create it:
```bash
terraform apply
```
- possibly can run into something like this: ```Error: googleapi: Error 403: The billing account for the owning project is disabled in state absent, accountDisabled```
    - if you followed zoomcamp in each module and create project for each topic/homework, free account has limited number of assignments per billing account --> remove or temporarly disable billing from one of existing projects
- run to remove everything and avoid potential billing of your account in the future:
```bash
terraform destroy
```

3) docker/docker compose installed
- create ```.env_encoded``` file
```
SECRET_GCP_SERVICE_ACCOUNT=GCP_SERVICE_ACCOUNT_JSON_ENCODED_IN_BASE64_UTF-8-CRLF
```
- replace ```GCP_SERVICE_ACCOUNT_JSON_ENCODED_IN_BASE64_UTF-8-CRLF``` with contents of ```service_account.json``` file encoded in base64, using UTF-8 encoding nad CRLF newline separator (e.g. base64encode.org/)

4) kestra kv setup
- spin up docker containers using:
```bash
docker-compose up -d
```
- visit ```localhost:8080/``` to access Kestra UI
- find and run flow ```gcp_kv_v2```

5) dbt core
- will add both dbt core and dbt bigquery connector
```bash
uv add dbt-bigquery
```
- now you will need ```profiles.yaml``` file, ideally in the project directory
- you can use and adjust existing ones if you used dbt with GCP before or create new one, either way, see below the content
```yaml
...
f1_analytics_dbt:
  outputs:
    dev:
      dataset: [GCP_DATASET_NAME]
      job_execution_timeout_seconds: 300
      job_retries: 1
      keyfile: [GCP_SERVICE_ACCOUNT_JSON_FILE_PATH]
      location: [GCP_PROJECT_LOCATION]
      method: service-account
      priority: interactive
      project: [GCP_PROJECT_ID]
      threads: 1
      type: bigquery
  target: dev
...
```
- to check the connection if it works
```bash
uv run dbt debug --project-dir f1_analytics_dbt --profiles-dir .
```
- you will also need to adjust the ```dbt_project.yml``` --> ```vars``` section according to your resources set up build by terraform
- --> that will be used in ```_sources.yaml``` in ```f1_analytics\models\stagging``` to be referenced in model building
```yaml
vars:
  gcp_project_id: [GCP_PROJECT_ID]
  bq_dataset: [GCP_DATASET_NAME]
```
- (local testing only; otherwise this step is done by Kestra) to build the models
```bash
uv run dbt build --project-dir f1_analytics_dbt --profiles-dir .
```

6) uv?
- ...

## How to run

1) terraform
- see above

2) docker
- see above

3) kestra
- ...