# Source data
source ~/spark/.bashrc

# Create Spark master
rm -rf $SPARK_HOME/logs/
$SPARK_HOME/sbin/start-master.sh --webui-port 9090
export MASTER=$(cat $SPARK_HOME/logs/spark-sera-org.apache.spark.deploy.master.Master-1-this-is-an-instance-test-stb.out | grep -o "spark://[^ ]*")
echo $MASTER

# Create Spark worker
$SPARK_HOME/sbin/start-slave.sh $MASTER --cores 4 --memory 10G

# Download Spark CCP Connector
gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar gcs-connector-hadoop3-2.2.5.jar
mv gcs-connector-hadoop3-2.2.5.jar $SPARK_HOME/jars/gcs-connector-hadoop3-2.2.5.jar
export GCP_CONNECTOR_LOCATION=${SPARK_HOME}/jars/gcs-connector-hadoop3-2.2.5.jar

# Export master to .env file
mkdir ~/etl_variables
touch ~/etl_variables/etl_variables.env
echo "MASTER=${MASTER}" >> ~/etl_variables/etl_variables.env
echo "GCP_CONNECTOR_LOCATION=${GCP_CONNECTOR_LOCATION}" >> ~/etl_variables/etl_variables.env