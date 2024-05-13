CONFIG = ./configscripts/config.conf
include ${CONFIG}


#propagateing config settings to the respective folders/files
refresh-confs:
	@cd configscripts && \
	sh refresh_confs.sh


#create infra with terraform - Note: you should be logged in with 'az login'
planinfra:
	@cd terraform && \
	terraform init --backend-config=backend.conf && \
	terraform plan -out terraform.plan

createinfra: planinfra
	@cd terraform && \
	terraform apply -auto-approve terraform.plan 

databricks-ws-config-export:
	@cd configscripts && \
	sh update_terraform_ws_configs.sh

#retrieve azure storage key and save it to a config file
retrieve-storage-keys:
	@echo "Retrieving azure keys"
	@cd configscripts && \
	sh retrieve_storage_keys.sh

#upload data to the provisioned storage account
uploaddata:
	@echo "Uploading data"
	@cd configscripts && \
	sh upload_data.sh


#destroy databricks workspace with terraform. Only the workspace, the data storage part remains	
destroy-databricks-ws:
	@cd terraform && \
	terraform destroy -auto-approve --target azurerm_databricks_workspace.bdcc
