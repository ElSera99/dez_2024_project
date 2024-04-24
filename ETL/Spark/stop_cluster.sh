# Source data
source ~/spark/.bashrc

# Stop Spark master
$SPARK_HOME/sbin/stop-master.sh

# Stop Spark worker
$SPARK_HOME/sbin/stop-slave.sh