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

## How to run

1) terraform
- see above

2) docker
- see above

3) kestra
- ...