#run this only after infrastructure is set up with terraform, and logged in with az login
#saves the storage account key to a config file
source ./config.conf

keys=$(az storage account keys list --resource-group $AZURE_RESOURCE_GROUP --account-name $STORAGE_ACCOUNT --query "[0].value" --output tsv)

if [ -n "$keys" ]; then
    echo STORAGE_ACCOUNT_KEY='"'$keys'"' > ./az_secret.conf
    echo "Storage Account Key retrieved and saved $STORAGE_ACCOUNT"
else
    echo "Failed to retrieve storage account key."
fi

