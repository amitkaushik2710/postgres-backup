## Backup -- dir name will be epoch time

## Step 1 
export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""
export AWS_REGION=""

## Step 2 
RUN cd backup
RUN go run main.go

## Restore

## Step 1
CREATE DATABASE IN DB
RUN CREATE DATABASE admindb;
RUN CREATE DATABASE agentdb;
RUN CREATE DATABASE userdb;

## Step 2
Run the svc to migrate tables

## Step 3
export S3_DIR="<epoch_directory_name>"

## Step 4
RUN cd restore
RUN go run main.go
