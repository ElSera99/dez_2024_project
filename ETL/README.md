# Setup
## VM Configuration
Once the Cloud infrastructure is created, you can continue with the configuration of the virtual machine.
In order to do so, a script is provided so you can only run it once inside the virtual machine.

This script is located in the folder **setup_vm**, copy the file ***setup.sh*** in the ```$HOME``` location inside the VM.

This script needs to setup the *environment variable* **GOOGLE_APPLICATION_CREDENTIALS**, which contains the location of the previously created **service account JSON credentials**. Make sure to copy the **JSON credentials file** into the VM and copy the **absolute path** to this credentials.

Now, **provide execution permissions** and run the **setup.sh** script as:
```bash
chmod +x setup.sh
./setup.sh <absolute_path_to_JSON_GOOGLE_APPLICATION_CREDENTIALS>
```
Finally, the VM is ready for usage

# Spark Configuration

# Airflow Configuration

# Job Execution