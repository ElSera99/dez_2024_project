# Get variables for execution
echo "Getting Variables for execution"
export GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}

export PROJECT_ID=$1 # dez-2024-project
export REGION=$2 # us-west1

export BUCKET_NAME=$3 # this-is-a-test-bucket
export DATASET_VERSION=$4 #v1.0
export INSTANCE_NAME=$5 # this-is-an-instance-test
export DATASET_NAME=$6 # my_dataset_test
echo ""

# Create bucket
echo "Create bucket for storage"
gcloud storage buckets create gs://${BUCKET_NAME} \
--project=${PROJECT_ID} \
--default-storage-class=STANDARD \
--location=${REGION} \
--uniform-bucket-level-access
echo ""

# Create medallion structure for bucket
echo "Create medallion structure for storage"
mkdir -p ${DATASET_VERSION}/bronze ${DATASET_VERSION}/silver ${DATASET_VERSION}/gold
touch ${DATASET_VERSION}/bronze/bronze.md ${DATASET_VERSION}/silver/silver.md ${DATASET_VERSION}/gold/gold.md
gcloud storage cp ./${DATASET_VERSION} gs://${BUCKET_NAME}/ --recursive
rm -rf ${DATASET_VERSION}
echo ""

# Create VM
echo "Create VM for processing"
gcloud compute instances create ${INSTANCE_NAME} \
    --project=${PROJECT_ID} \
    --zone=${REGION}-a \
    --machine-type=c2-standard-8 \
    --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --no-service-account \
    --no-scopes \
    --tags=http-server,https-server \
    --create-disk=auto-delete=yes,boot=yes,device-name=test-instance-dez,image=projects/ubuntu-os-cloud/global/images/ubuntu-2004-focal-v20240307b,mode=rw,size=100,type=projects/dez-2024-project/zones/${REGION}-a/diskTypes/pd-balanced \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --labels=goog-ec-src=vm_add-gcloud \
    --reservation-affinity=any
echo ""

# Create BigQuery dataset
echo "Create BigQuery dataset"
export VERSION=$( echo ${DATASET_VERSION} | tr '.' '_' ) 
export BQ_DATASET_NAME=${DATASET_NAME}_${VERSION}
gcloud alpha bq datasets create ${BQ_DATASET_NAME}  \
--project=${PROJECT_ID} \
--description 'Dataset for pipeline storage' \
--overwrite
echo ""

# Export variable names
echo "Export project variables"
export ENV_DIRECTORY=~/project_variables/${DATASET_VERSION}
mkdir -p $ENV_DIRECTORY
touch $ENV_DIRECTORY/project_variables.env
echo "GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}" >> $ENV_DIRECTORY/project_variables.env
echo "PROJECT_ID=${PROJECT_ID}" >> $ENV_DIRECTORY/project_variables.env
echo "REGION=${REGION}" >> $ENV_DIRECTORY/project_variables.env
echo "BUCKET_NAME=${BUCKET_NAME}" >> $ENV_DIRECTORY/project_variables.env
echo "INSTANCE_NAME=${INSTANCE_NAME}" >> $ENV_DIRECTORY/project_variables.env
echo "DATASET_NAME=${DATASET_NAME}" >> $ENV_DIRECTORY/project_variables.env
echo "DATASET_VERSION=${DATASET_VERSION}" >> $ENV_DIRECTORY/project_variables.env
echo "BQ_DATASET_NAME=${BQ_DATASET_NAME}" >> $ENV_DIRECTORY/project_variables.env
echo ""
