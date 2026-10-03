# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # RetailCo — Parameterized Customer Sales Report
# MAGIC
# MAGIC **Purpose:** Demonstrate notebook parameterization in Databricks using `dbutils.widgets`.
# MAGIC
# MAGIC This notebook can be reused for different cities, dates, minimum sales amounts,
# MAGIC and order statuses without changing the notebook code.
# MAGIC
# MAGIC ## Parameters
# MAGIC - `city` — City to process
# MAGIC - `report_date` — Sales date
# MAGIC - `min_amount` — Minimum sale amount
# MAGIC - `status` — Order status
# MAGIC
# MAGIC **Real-time company use:** A Databricks Job or Azure Data Factory can pass these
# MAGIC values at runtime. The same notebook can therefore process many dates,
# MAGIC customers, cities, or environments.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Remove existing widgets
# MAGIC
# MAGIC Run this when developing interactively so old widget definitions do not remain.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 — Create notebook parameters
# MAGIC
# MAGIC These widgets allow values to be supplied at runtime.

# COMMAND ----------

dbutils.widgets.text("city", "Hyderabad", "City")
dbutils.widgets.text("report_date", "2026-09-28", "Report Date (YYYY-MM-DD)")
dbutils.widgets.text("min_amount", "5000", "Minimum Sale Amount")
dbutils.widgets.dropdown(
    "status",
    "Completed",
    ["Completed", "Pending", "Cancelled", "All"],
    "Order Status"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3 — Read the parameters
# MAGIC
# MAGIC `dbutils.widgets.get()` reads the value supplied to the notebook.
# MAGIC
# MAGIC **Important:** widget values are returned as strings, so numeric values
# MAGIC should be converted before numeric comparisons.

# COMMAND ----------

city = dbutils.widgets.get("city")
report_date = dbutils.widgets.get("report_date")
min_amount = int(dbutils.widgets.get("min_amount"))
status = dbutils.widgets.get("status")

print("=" * 50)
print("PARAMETERS RECEIVED")
print("=" * 50)
print(f"City        : {city}")
print(f"Report Date : {report_date}")
print(f"Min Amount  : ₹{min_amount:,}")
print(f"Status      : {status}")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4 — Create sample sales data
# MAGIC
# MAGIC In a real company, this data could come from an ADLS/Delta table.
# MAGIC For classroom practice, we create a small DataFrame.

# COMMAND ----------

from pyspark.sql.functions import col, to_date, lit

data = [
    (1, "Hyderabad", "2026-09-28", 8500, "Completed"),
    (2, "Mumbai", "2026-09-28", 12000, "Completed"),
    (3, "Hyderabad", "2026-09-28", 3200, "Pending"),
    (4, "Bangalore", "2026-09-28", 15000, "Completed"),
    (5, "Hyderabad", "2026-09-27", 7800, "Completed"),
    (6, "Chennai", "2026-09-28", 9100, "Cancelled"),
    (7, "Hyderabad", "2026-09-28", 6200, "Completed"),
    (8, "Pune", "2026-09-28", 4300, "Completed"),
    (9, "Mumbai", "2026-09-27", 5500, "Pending"),
    (10, "Bangalore", "2026-09-28", 11200, "Completed")
]

columns = ["order_id", "city", "sale_date", "total_amount", "status"]

df = spark.createDataFrame(data, columns)
df = df.withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5 — Use the parameters in the transformation
# MAGIC
# MAGIC The notebook code stays the same. Only the parameter values change.

# COMMAND ----------

df_filtered = df.filter(col("city") == city)

df_filtered = df_filtered.filter(
    col("sale_date") == to_date(lit(report_date), "yyyy-MM-dd")
)

df_filtered = df_filtered.filter(col("total_amount") >= min_amount)

if status != "All":
    df_filtered = df_filtered.filter(col("status") == status)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6 — Display the result

# COMMAND ----------

record_count = df_filtered.count()

print("=" * 50)
print("FILTERED SALES REPORT")
print("=" * 50)
print(f"City        : {city}")
print(f"Report Date : {report_date}")
print(f"Min Amount  : ₹{min_amount:,}")
print(f"Status      : {status}")
print(f"Records     : {record_count}")
print("=" * 50)

display(df_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7 — Classroom experiments
# MAGIC
# MAGIC Try changing only the widget values.
# MAGIC
# MAGIC **Experiment 1**
# MAGIC - City = `Mumbai`
# MAGIC - Report Date = `2026-09-28`
# MAGIC - Minimum Amount = `5000`
# MAGIC - Status = `Completed`
# MAGIC
# MAGIC **Experiment 2**
# MAGIC - City = `Hyderabad`
# MAGIC - Report Date = `2026-09-28`
# MAGIC - Minimum Amount = `3000`
# MAGIC - Status = `All`
# MAGIC
# MAGIC Notice that the notebook code does not need to change.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8 — How companies use notebook parameterization
# MAGIC
# MAGIC ### Example 1 — Daily processing
# MAGIC A Job passes `report_date` every day.
# MAGIC
# MAGIC ### Example 2 — Multiple cities
# MAGIC The same notebook can process Hyderabad, Mumbai, Chennai, etc.
# MAGIC
# MAGIC ### Example 3 — Dev / Test / Production
# MAGIC A parameter such as `environment` can determine which storage location
# MAGIC or configuration the notebook uses.
# MAGIC
# MAGIC ### Example 4 — Full vs Incremental load
# MAGIC A parameter such as `load_type` can tell the notebook whether to process
# MAGIC all records or only new/changed records.
# MAGIC
# MAGIC **Key idea:**
# MAGIC
# MAGIC `One Notebook + Different Parameters = Reusable Notebook`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9 — Databricks Job example
# MAGIC
# MAGIC A Databricks Job can run this notebook and provide values such as:
# MAGIC
# MAGIC ```text
# MAGIC city        = Hyderabad
# MAGIC report_date = 2026-09-28
# MAGIC min_amount  = 5000
# MAGIC status      = Completed
# MAGIC ```
# MAGIC
# MAGIC Tomorrow, the same Job can pass a different date or city.
# MAGIC The notebook code remains unchanged.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10 — Azure Data Factory example
# MAGIC
# MAGIC ADF can call the Databricks notebook and pass Base Parameters.
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC ```text
# MAGIC city        → pipeline parameter: city
# MAGIC report_date → pipeline parameter: report_date
# MAGIC min_amount  → pipeline parameter: min_amount
# MAGIC status      → pipeline parameter: status
# MAGIC ```
# MAGIC
# MAGIC This allows an ADF pipeline to control the behavior of the same
# MAGIC reusable Databricks notebook.

# COMMAND ----------

#Return result to ADF
dbutils.notebook.exit(str(record_count))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaching summary
# MAGIC
# MAGIC **Parameterization means passing values into a notebook at runtime
# MAGIC instead of hard-coding those values inside the notebook.**
# MAGIC
# MAGIC Common company parameters:
# MAGIC - Date
# MAGIC - File path
# MAGIC - Customer
# MAGIC - City
# MAGIC - Environment
# MAGIC - Load type
# MAGIC - Status
# MAGIC
# MAGIC **Main benefit:** Reusability, easier maintenance, and less duplicate code.