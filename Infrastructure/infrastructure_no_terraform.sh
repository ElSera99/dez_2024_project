wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-472.0.0-linux-x86_64.tar.gz
gunzip google-cloud-cli-472.0.0-linux-x86_64.tar.gz
tar -xf google-cloud-cli-472.0.0-linux-x86_64.tar

export GOOGLE_APPLICATION_CREDENTIALS=keys_path

gcloud compute instances create test-instance-dez \
    --project=dez-2024-project \
    --zone=us-west1-b \
    --machine-type=c2-standard-8 \
    --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --no-service-account \
    --no-scopes \
    --tags=http-server,https-server \
    --create-disk=auto-delete=yes,boot=yes,device-name=test-instance-dez,image=projects/ubuntu-os-cloud/global/images/ubuntu-2004-focal-v20240307b,mode=rw,size=100,type=projects/dez-2024-project/zones/us-west1-b/diskTypes/pd-balanced \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --labels=goog-ec-src=vm_add-gcloud \
    --reservation-affinity=any

    