
databricks fs cp --overwrite ../target/azure-databricks-listeners-0.9.jar dbfs:/azure-databricks-job/azure-databricks-listeners-0.9.jar
databricks fs cp --overwrite spark-listeners.sh dbfs:/databricks/init/<cluster-name>/spark-listeners.sh

