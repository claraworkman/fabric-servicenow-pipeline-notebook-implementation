# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c9c5a760-852c-4463-86f4-e3f123d7581c",
# META       "default_lakehouse_name": "servicenow_lakehouse",
# META       "default_lakehouse_workspace_id": "770909c8-52d5-443e-a0ca-4712cfa36884",
# META       "known_lakehouses": [
# META         {
# META           "id": "c9c5a760-852c-4463-86f4-e3f123d7581c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # ServiceNow Watermark Check
# ### Queries the Lakehouse table for the maximum `sys_updated_on` value and returns it to the pipeline.

# CELL ********************

# Parameter injected by the pipeline ForEach activity
table_name = "incident"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

df = spark.sql(f"SELECT MAX(sys_updated_on) AS watermark FROM {table_name}")
watermark = df.collect()[0]["watermark"]

if watermark is None:
    result = "1970-01-01 00:00:00"
else:
    result = str(watermark)

mssparkutils.notebook.exit(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
