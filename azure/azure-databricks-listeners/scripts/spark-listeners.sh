#!/bin/bash

STAGE_DIR=/dbfs/azure-databricks-job
echo "Copying listener jar"
cp -f "$STAGE_DIR/azure-databricks-listeners-0.9.jar" /mnt/driver-daemon/jars || { echo "Error copying file"; exit 1;}
echo "Copied listener jar successfully"

