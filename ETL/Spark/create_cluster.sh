# Source data
source ~/spark/.bashrc

# Create Spark master
rm -rf $SPARK_HOME/logs/
$SPARK_HOME/sbin/start-master.sh
export MASTER=$(cat $SPARK_HOME/logs/spark-sera-org.apache.spark.deploy.master.Master-1-this-is-an-instance-test-stb.out | grep -o "spark://[^ ]*")
echo $MASTER

# Create Spark worker
$SPARK_HOME/sbin/start-slave.sh $MASTER

# Export master to .env file
mkdir ~/etl_variables
touch ~/etl_variables/etl_variables.env
echo "MASTER=${MASTER}" >> ~/etl_variables/etl_variables.env