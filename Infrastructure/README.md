# Infrastructure
This section specifies how infrastructure can be built by using **IaC** with two posible options.
The following resources will be allocated in order to execute the project:
* Compute Engine: VM Instance
* Cloud Storage: Standard Bucket
* BigQuery: Versioned dataset

# Setup GCP Account
In order to follow this project, a **GCP free tier account** must be created as established in:
- https://cloud.google.com/free

Also, a **Service Account** must be created as specified in:
- https://cloud.google.com/iam/docs/service-accounts-create

And, finally, a **JSON Credentials File** is needed to make use of provided **IaC**, this file can be created as specified in:
- https://cloud.google.com/iam/docs/keys-create-delete


# Create Infrastructure
## No Terraform
This option is used when the usage of terraform cannot be executed, a bash script that uses **gcloud cli utility** is provided, located in the folder *gcloud_cli* it is named as **setup_infrastructure.sh**

It is implied that a **GCP account** and its respective **JSON credentials** are created and stored in the machine that will run the script as described in:
- https://cloud.google.com/docs/authentication/application-default-credentials#GAC

Also, the respective **authentication process** is executed as described in::
- https://cloud.google.com/sdk/gcloud/reference/auth/login

This script takes as input **6 arguments** that are used to correctly setup the infrastructure, arguments are as follows:
1. PROJECT_ID: Obtained from **GCP** dashboard, can look as:*dez-2024-project*
2. REGION: Obtained from available [regions](https://cloud.google.com/compute/docs/regions-zones) provided by GCP, in this region the infrastructure will be built, can look as: *us-west1*
3. BUCKET_NAME: Name that will be given to the bucket, make sure to follow naming conventions, can look as: *this-is-a-test-bucket*
4. DATASET_VERSION: Used to **version** infrastructure for bucket and dataset in BigQuery, can look as: *v1.0*
5. INSTANCE_NAME: Name that will be given to the VM instance that will be used for processing, can look like this: *this-is-an-instance-test*
6. DATASET_NAME: Name that will be given to the dataset in BigQuery, can look like this:*my_dataset_test*, version will be appended, hence, dataset name will also take given version and name will end up as: *my_dataset_test_v1_0*

Execution example:

Make sure to **provide execution permissions** before running:
```bash
chmod +x setup_infrastructure.sh
```

Execute as:
```bash
./setup_infrastructure.sh \
dez-2024-project \
us-west1 \
this-is-a-test-bucket \
v1.0 \
this-is-an-instance-test \
my_dataset_test
```

After execution, infrastructure will be created and ready to be used.

## Terraform


# VM Configuration
Once the Cloud infrastructure is created, you can continue with the configuration of the virtual machine.
In order to do so, a script is provided so you can only run it once inside the virtual machine.

This script is located in the folder **setup_vm**, copy the file ***setup.sh*** in the ```$HOME``` location inside the VM.

This script needs to setup the *environment variable* **GOOGLE_APPLICATION_CREDENTIALS**, which contains the location of the previously created **service account JSON credentials**. Make sure to copy the **JSON credentials file** into the VM and copy the **absolute path** to this credentials.

Now, **provide execution permissions** and run the **setup.sh** script as:
```bash
chmod +x setup.sh
./setup.sh <absolute_path_to_JSON_GOOGLE_APPLICATION_CREDENTIALS>
```