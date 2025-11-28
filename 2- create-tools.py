# Databricks notebook source
# MAGIC %pip install unitycatalog-ai[databricks]
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

catalog_name = "agents_lab"
schema_name = "product"
dbutils.widgets.text("catalog_name", defaultValue=catalog_name, label="Catalog Name")
dbutils.widgets.text("schema_name", defaultValue=schema_name, label="Schema Name")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Tools

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Get Latest Return

# COMMAND ----------

# DBTITLE 1,Get the Latest Return in the Processing Queue
# MAGIC %sql
# MAGIC -- Select the date of the interaction, issue category, issue description, and customer name
# MAGIC SELECT 
# MAGIC   cast(date_time as date) as case_time, 
# MAGIC   issue_category, 
# MAGIC   issue_description, 
# MAGIC   name
# MAGIC FROM agents_lab.product.cust_service_data 
# MAGIC -- Order the results by the interaction date and time in descending order
# MAGIC ORDER BY date_time DESC
# MAGIC -- Limit the results to the most recent interaction
# MAGIC LIMIT 1

# COMMAND ----------

# DBTITLE 1,Create a function registered to Unity Catalog
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION 
# MAGIC   IDENTIFIER(:catalog_name || '.' || :schema_name || '.get_latest_return')()
# MAGIC RETURNS TABLE(purchase_date DATE, issue_category STRING, issue_description STRING, name STRING)
# MAGIC COMMENT 'Returns the most recent customer service interaction, such as returns.'
# MAGIC RETURN (
# MAGIC   SELECT 
# MAGIC     CAST(date_time AS DATE) AS purchase_date,
# MAGIC     issue_category,
# MAGIC     issue_description,
# MAGIC     name
# MAGIC   FROM agents_lab.product.cust_service_data
# MAGIC   ORDER BY date_time DESC
# MAGIC   LIMIT 1
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Test function call to retrieve latest return
# MAGIC %sql
# MAGIC select * from IDENTIFIER(:catalog_name || '.' || :schema_name || '.get_latest_return')()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Get Company Policies

# COMMAND ----------

# DBTITLE 1,Create function to retrieve return policy
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION
# MAGIC   IDENTIFIER(:catalog_name || '.' || :schema_name || '.get_return_policy')()
# MAGIC RETURNS TABLE (
# MAGIC   policy           STRING,
# MAGIC   policy_details   STRING,
# MAGIC   last_updated     DATE
# MAGIC )
# MAGIC COMMENT 'Returns the details of the Return Policy'
# MAGIC LANGUAGE SQL
# MAGIC RETURN (
# MAGIC   SELECT
# MAGIC     policy,
# MAGIC     policy_details,
# MAGIC     last_updated
# MAGIC   FROM agents_lab.product.policies
# MAGIC   WHERE policy = 'Return Policy'
# MAGIC   LIMIT 1
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Test function to retrieve return policy
# MAGIC %sql
# MAGIC select * from IDENTIFIER(:catalog_name || '.' || :schema_name || '.get_return_policy')()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Look Up the Order History by User Name 

# COMMAND ----------

# DBTITLE 1,Create function that retrieves order history based on userID
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION
# MAGIC   IDENTIFIER(:catalog_name || '.' || :schema_name || '.get_order_history')(user_name STRING)
# MAGIC RETURNS TABLE (returns_last_12_months INT, issue_category STRING, todays_date DATE)
# MAGIC COMMENT 'This takes the user_name of a customer as an input and returns the number of returns and the issue category'
# MAGIC LANGUAGE SQL
# MAGIC RETURN 
# MAGIC SELECT count(*) as returns_last_12_months, issue_category, now() as todays_date
# MAGIC FROM agents_lab.product.cust_service_data 
# MAGIC WHERE name = user_name 
# MAGIC GROUP BY issue_category;

# COMMAND ----------

# DBTITLE 1,Test function that retrieves order history based on userID
# MAGIC %sql
# MAGIC select * from IDENTIFIER(:catalog_name || '.' || :schema_name || '.get_order_history')('Nicolas Pelaez')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Python functions

# COMMAND ----------

# DBTITLE 1,Very simple Python function
def get_todays_date() -> str:
    """
    Returns today's date in 'YYYY-MM-DD' format.

    Returns:
        str: Today's date in 'YYYY-MM-DD' format.
    """
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d")

# COMMAND ----------

# DBTITLE 1,Test python function
today = get_todays_date()
today

# COMMAND ----------

# DBTITLE 1,Register python function to Unity Catalog
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient()

python_tool_uc_info = client.create_python_function(func=get_todays_date, catalog=catalog_name, schema=schema_name, replace=True)
