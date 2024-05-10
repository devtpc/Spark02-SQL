#This script copies/creates config files in subfolders
source ./config.conf

#terraform backend
echo "Copying terraform backend config"
cp -v terraform_backend.conf ../terraform/backend.conf

#terraform azure names
echo "Creating terraform tfvars to ../terraform/terraform.auto.tfvars"
echo ENV = '"'$AZURE_BASE'"' > ../terraform/terraform.auto.tfvars
echo LOCATION = '"'$AZURE_LOCATION'"' >> ../terraform/terraform.auto.tfvars

