cd ../terraform
echo "DATABRICKS_HOST=$(terraform output -raw databricks_host)" > ../configscripts/az_databricks_config.conf && \
echo "DATABRICKS_WORKSPACE_ID=$(terraform output -raw databricks_workspace_id)" >> ../configscripts/az_databricks_config.conf