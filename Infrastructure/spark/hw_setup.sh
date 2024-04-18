# Update if necessary
sudo apt-get update
sudo apt-get upgrade

# Get Docker
sudo apt-get install docker.io
sudo groupadd docker
sudo gpasswd -a $USER docker
sudo service docker restart

# Setup Google Credentials
export GOOGLE_APPLICATION_CREDENTIALS=<path_to_credentials>
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS

# Get pip and venv
wget https://bootstrap.pypa.io/get-pip.py
python3 get-pip.py
python3 -m pip install --upgrade pip setuptools
rm get-pip.py
sudo apt-get instal python3.8-venv

# Create spark directory
cd ~
mkdir spark
cd spark

# Create .bashrc file
touch .bashrc

# Get Java SDK
wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz
rm openjdk-11.0.2_linux-x64_bin.tar.gz

# Create environment variables for Java
echo 'export JAVA_HOME="${HOME}/spark/jdk-11.0.2"' >> .bashrc
echo 'export PATH="${JAVA_HOME}/bin:${PATH}"' >> .bashrc

# Add blanck space
echo >> .bashrc

# Get Spark
wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
tar xzfv spark-3.3.2-bin-hadoop3.tgz
rm spark-3.3.2-bin-hadoop3.tgz

# Create environment variables for Spark
echo 'export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"' >> .bashrc
echo 'export PATH="${SPARK_HOME}/bin:${PATH}"' >> .bashrc

# Add blank space
echo >> .bashrc

# Set variables for PySpark
echo 'export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"' >> .bashrc
echo 'export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"' >> .bashrc
