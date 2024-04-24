# Update if necessary
echo "Get latest updates for Ubuntu system"
sudo apt-get update
sudo apt-get upgrade -y
echo ""

# Get compressed files utilities
echo "Instalinc compressed files utilities"
sudo apt-get install tar -y
sudo apt-get install gzip -y
echo ""

# Get Docker
echo "Install Docker utilities"
sudo apt-get install docker.io -y
sudo groupadd docker
sudo gpasswd -a $USER docker
sudo service docker restart
DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}
mkdir -p $DOCKER_CONFIG/cli-plugins
curl -SL https://github.com/docker/compose/releases/download/v2.26.1/docker-compose-linux-x86_64 -o $DOCKER_CONFIG/cli-plugins/docker-compose
chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose
echo ""

# Install glcoud cli
echo "Install gcloud cli utilites"
sudo apt-get install apt-transport-https ca-certificates gnupg curl -y
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg -y | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg -y
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
sudo apt-get update && sudo apt-get install google-cloud-cli -y
echo ""

# Setup Google credentials
echo "Create GCP credentials initialization"
export GOOGLE_APPLICATION_CREDENTIALS=$1
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
echo ""

# Get pip and venv
echo "Fix Python possible missing pip"
wget https://bootstrap.pypa.io/get-pip.py
python3 get-pip.py
python3 -m pip install --upgrade pip setuptools
rm get-pip.py
sudo apt-get install python3.8-venv -y
echo ""

# Create spark directory
echo "Start Spark local configuration"
cd ~
mkdir spark
cd spark
echo ""

# Create .bashrc file
echo "Create .bashrc file"
touch .bashrc
echo ""

# Add GCP credentials to bashrc
echo "Add GCP credentials to bashrc"
echo "export GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}" >> .bashrc
echo >> .bashrc
echo ""

# Get Java SDK
echo "Get Java SDK"
wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
gunzip openjdk-11.0.2_linux-x64_bin.tar.gz
tar -xf openjdk-11.0.2_linux-x64_bin.tar
rm openjdk-11.0.2_linux-x64_bin.tar
echo ""

# Create environment variables for Java
echo "Set JAVA variables to bashrc"
echo 'export JAVA_HOME="${HOME}/spark/jdk-11.0.2"' >> .bashrc
echo 'export PATH="${JAVA_HOME}/bin:${PATH}"' >> .bashrc
echo >> .bashrc
echo ""

# Get Spark
echo "Get Spark binaries"
wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
tar -xf spark-3.3.2-bin-hadoop3.tgz
rm spark-3.3.2-bin-hadoop3.tgz
echo ""

# Create environment variables for Spark
echo "Set Spark variables to bashrc"
echo 'export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"' >> .bashrc
echo 'export PATH="${SPARK_HOME}/bin:${PATH}"' >> .bashrc
echo >> .bashrc
echo 'export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"' >> .bashrc
echo 'export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"' >> .bashrc
echo ""

# Create Python virtual environment
echo "Create Python virtual environment"
python3 -m venv venv
source venv_test/bin/activate
python3 -m pip install jupyterlab pandas pyarrow 
echo ""
