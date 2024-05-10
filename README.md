# Spark SQL Homework

## Introduction

This project is a homework at the EPAM Data Engineering Mentor program. The main idea behind this task is to create a Spark job using Azure Databricks. Infrastructure should be set up using Terraform. The original copyright belongs to [EPAM](https://www.epam.com/). 

#### Some original instructions about the task:

* Download/fork the backbone project
* Download the [data files](https://static.cdn.epam.com/uploads/583f9e4a37492715074c531dbd5abad2/dse/m07sparksql.zip). Unzip it and upload into provisioned via Terraform storage account.
* Copy hotel/weather and Expedia data from local into provisioned with terraform Storage Account.
* Create Databricks Notebooks (storage libraries are already part of Databricks environment).
* Create delta tables based on data in storage account.
* Using Spark SQL calculate and visualize in Databricks Notebooks 
  - Top 10 hotels with max absolute temperature difference by month.
  - Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.
  -  For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.
* For designed queries analyze execution plan. Map execution plan steps with real query. Specify the most time (resource) consuming part. For each analysis you could create tables with proper structure and partitioning if necessary.
* Deploy Databricks Notebook on cluster, to setup infrastructure use terraform scripts from module.
* Store final DataMarts and intermediate data (joined data with all the fields from both datasets) in provisioned with terraform Storage Account
* Expected results
  - Repository with notebook (with output results), configuration scripts, application sources, execution plan dumps, analysis etc.
  - Upload in task Readme MD file with link on repo, fully documented homework with screenshots and comments.




## About the repo

This repo is hosted [here](https://github.com/devtpc/Spark02-SQL)

> [!NOTE]
> The original data files are not included in this repo, only the link.
> Some sensitive files, like configuration files, API keys, tfvars are not included in this repo.


## Prerequisites

* The necessiary software environment should be installed on the computer ( azure cli, terraform, etc.)
* For Windows use Gitbash to run make and shell commands. (Task was deployed and tested on a Windows machine)
* Have an Azure account (free tier is enough)
* Have an Azure storage account ready for hosting the terraform backend


## Preparatory steps

### Download the data files

Download the data files from [here](https://static.cdn.epam.com/uploads/583f9e4a37492715074c531dbd5abad2/dse/m07sparksql.zip).
Exctract the zip file, and copy its content to this repo. Rename the `m07sparksql` folder to `input`, and put it in a folder named `data`.
The file structure should look like this:

![File structure image](/screenshots/img_data_file_structure.png)

### Setup your configuration

Go to the [configcripts folder](/configscripts/) and copy/rename the `config.conf.template` file to `config.conf`. Change the AZURE_BASE, AZURE_LOCATION, and other values as instructed within the file.

In the [configcripts folder](/configscripts/) copy/rename the `terraform_backend.conf.template` file to `terraform_backend.conf`. Fill the parameters with the terraform data.

Propagate your config data to other folders with the [refreshconfs.sh](/configscripts/refresh_confs.sh) script, or with `make refresh-confs` from the main folder

The details are in comments in the config files.

## Creating the Databricks environment

Before starting, make sure, that config files were correctly set up and broadcasted in the 'Setup your configuration'. There should be a terraform.auto.tvars file with your azure config settings, and a backend.conf file with the backend settings in the terraform folder.

Log in to Azure CLI with `az login`



### Setup the Azure base infrastructure with terraform

Use `make createinfra` command to create your azure infrastructure. Alternatively, to do it step-by-step:

In your terraform folder:
```
#initialize your terraform
terraform init --backend-config=backend.conf

#plan the deployment
terraform plan -out terraform.plan

#confirm and apply the deployment. If asked, answer yes.
terraform apply terraform.plan
```
After running the script, you get a message, that 'Apply complete'. The following picture shows a modification, if you run it for the first time, all resources would be in the 'added' category.

![Terraform created](/screenshots/img_terraform_created.png)


### Optional: Verify the infrastructure
To verify the infrastructure visually, login to the Azure portal, and view your resource groups. There are  2 new resource groups:
* the one, which was parameterized, named rg-youruniquename-yourregion, with the Azure Databricks Service and the Storage account.

![Databricks created](/screenshots/img_databricks_created.png)

* Databricks' own resources, starting with databrics-rg-

![Databricks created 2](/screenshots/img_databricks_created_2.png)

After entering the Databricks Service, you can launch the workspace, confirming that it is really working:

![Databricks created 3](/screenshots/img_databricks_created_3.png)

The data container was also created:

![Databricks created 4](/screenshots/img_databricks_created_4.png)

### Save needed keys from Azure
Storage access key will be needed in configuring the cluster, so save the storage account key from Azure by typing `make retrieve-storage-keys`. This saves the storage key to `configscripts/az_secret.conf` file. For retrieving, the command uses internally the following structure:
```
keys=$(az storage account keys list --resource-group $AKS_RESOURCE_GROUP --account-name $STORAGE_ACCOUNT --query "[0].value" --output tsv)
```

### Upload data input files to the storage account

Now, that you have your storage account and key ready, the data files can be uploaded to the storage. Type `make uploaddata` to upload the data files. This command uses internally the following structure:

```
az storage blob upload-batch --source ./../data/input --destination data/input  --account-name $STORAGE_ACCOUNT --account-key $STORAGE_ACCOUNT_KEY
```

The data is uploaded to the server:

![Data upload 1](/screenshots/img_dataupload_1.png)

![Data upload 2](/screenshots/img_dataupload_2.png)


### Create a Databricks cluster manually

To create the databricks cluster, manually create the cluster at the 'Compute' screen:

![Create Cluster 1](/screenshots/img_create_cluster_1.png)

At the advanced option set the storage account's key at the sparkconfig, and the account name and container name as environment variables, as in the picture below.

![Create Cluster 2](/screenshots/img_create_cluster_2.png)

### Create a Databricks cluster with Terraform


> [!NOTE]
> The cluster can be set up with Terraform as well, however the Azure free Tier's limitations did not make it possibe for me to set it up this way. A more detailed explanation is [here](/texts/limitations/README.md)


## Running the app

Upload [the notebook](/notebooks/sparktask_02.sql) as a source to the Azure Databricks portal to your workspace, and run the cells. The notebook can be run cell-by-cell, or all cells can be run together. The executed notebook can be viewed with the result visualizations [here (HTML version)](/notebooks/sparktask_02.html) without Databricks .

The notebook is documented in detail with comments, so here I include only the main steps. 

### Start the execution

The beginning starts with reading the configurations. Based on the configurations a new database schema is created, named `datalake`

![Schema created](/screenshots/img_schema_created.png)

The input data should be converted to delta tables, which are to be stored as external tables in the delta lake.

![Deltalake created 1](/screenshots/img_deltalake_create_1.png)

It can be verifyied visually, that the new files were created in the storage account.
![Deltalake created 2](/screenshots/img_deltalake_create_2.png)

For the small, simple queries there's no need to analyze the execution plan. However, there are several more complicated queries (with joins, filters, nested queries) which are worth to be analyzed, as the original task requested it.

### Task 1: Top 10 hotels with max absolute temperature difference by month

#### Execution plan

Here we have some simple nested query, window function, and filtering, which is worth to analyze. This can be done by writing `EXPLAIN EXTENDED` before the query.

![Explain Task 1](/screenshots/img_explain_task1.png)

We can see the [execution plan](/texts/explains/task1/task1_plan.txt), the [executed plan](/texts/explains/task1/task1_result.txt), and the executed [Visual Directed Acyclic Graph](/texts/explains/task1/task1_dag.png)

![Visual Directed Acyclic Graph](/texts/explains/task1/task1_dag.png)

From this graph and the plans we can see, that is was a simple linear task.

#### Result, Visualization

Although the Databricks Notebook has inbuilt visualization possibility, Python and seaborn was used the create visualization, mainly because multiple visualization were needed in each task. In Task 1 seperate charts were created for each month.

![Task 1 viz](/screenshots/img_viz_task1.png)

### Task 2: Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.

#### Execution plan
Similarly to Task 1, one complicated query was analyzed. The query containes a CROSS JOIN, which is generally very expensive, however in this case one of the tables had only 3 rows.

This time the Execution plan is not completely linear:

![Visual Directed Acyclic Graph 2](/texts/explains/task1/task2_dag.png)

Here are the [execution plan](/texts/explains/task2/task2_plan.txt) and the [executed plan](/texts/explains/task2/task2_result.txt)

Some trivial optimization were performed, like creating `is not null` filters before the joins.

#### Result, Visualization

The results were visualized as 3 horizontal bar charts.

![Task 2 viz](/screenshots/img_viz_task2.png)

### Task 3: For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay

At this task there were two, moderately complicated queries, which were worth to be analyzed. The two queries could have been written as a single, more complicated query, but as it was also written in the original task description, that among the intermediate queries, the joined data from both datasets should be stored, I decided to implement it as two different queries.

#### Subquery 1: Create joined hotel/weather - expedia data

Here are the [execution plan](/texts/explains/task3/task3_s1_plan.txt) and the [executed plan](/texts/explains/task3/task3_s1_result.txt)

If we analyze the DAG and the execution plan, we can notice, that at this stage an important optimization was performed:
* In the Analyzed Logical Plan all filters were applied after the join
* In the Optimized Logical Plan and the Physical Plan not only the trivial `is not null` filters, but other important filters were also moved before the join, substantially enhancing the performance of the query.

![Visual Directed Acyclic Graph 3/1](/texts/explains/task3/task3_s1_dag.png)


#### Subquery 2: Calculating weather trends for the extended stays

This query's [execution plan](/texts/explains/task3/task3_s2_plan.txt) and [executed plan](/texts/explains/task3/task3_s2_result.txt) may seem a bit complicated at first glance, but it's mainly because there is an aggregation and 3 joins. However, in practice it's relatively cheap to implement, as after the aggregations, the joins are adding only new columns, not inflating the number of rows.


![Visual Directed Acyclic Graph 3/2](/texts/explains/task3/task3_s2_dag.png)


#### Result, Visualization

The main result of the query is a row for every extended stay booking, containing the average temperatures and the weather trends. We visualize tre result with a scatterplot, and 2 histograms:


![Task 3 viz](/screenshots/img_viz_task3.png)

As it can be seen on the scatterplot and the histogram for weather trend, there is a siginicant portion of the data, where although the booking is for more than 7 days, the weather trend is 0 change in temperature. Its main reason is, that in the original weather data many data are missing, so even the bookings were long, in some cases ony 1 day of weather data was included.


### End result

If we look at the files on our storage account, it can be seen, that as we used external data tables, the intermediate data were stored in the storage.

![Deltalake files](/screenshots/img_deltalake_end.png)


### Destroy the Databricks workspace

Run `make destroy-databricks-ws` to destroy the Databricks workspace. This cleanes up the workspace, but preserves the result in our storage.
