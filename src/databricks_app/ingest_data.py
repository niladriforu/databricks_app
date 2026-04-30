# PySpark boilerplate to execute SQL query and load into DataFrame
from pyspark.sql.functions import col

query = """
SELECT * 
FROM samples.bakehouse.sales_transactions a
INNER JOIN samples.bakehouse.sales_customers b ON a.customerID = b.customerID
INNER JOIN samples.bakehouse.sales_franchises c ON a.franchiseID = c.franchiseID
INNER JOIN samples.bakehouse.media_gold_reviews_chunked d ON c.franchiseID = d.franchiseID
INNER JOIN samples.bakehouse.media_customer_reviews e ON e.franchiseID = c.franchiseID
"""

# Execute the query and load results into a DataFrame
# df = spark.sql(query)

sales_transactions_df = spark.table("samples.bakehouse.sales_transactions")
# display(sales_transactions_df)

# Rename 'name' to 'supplier_name' to avoid ambiguity
sales_suppliers_df = spark.table("samples.bakehouse.sales_suppliers").withColumnRenamed("name", "supplier_name")
# display(sales_suppliers_df)

sales_customers_df = spark.table("samples.bakehouse.sales_customers").withColumnRenamed("name", "customer_name")
# display(sales_customers_df)

# Rename 'name' to 'franchise_name' to avoid ambiguity
sales_franchises_df = spark.table("samples.bakehouse.sales_franchises").withColumnRenamed("name", "franchise_name")
# display(sales_franchises_df)

media_gold_reviews_chunked_df = spark.table("samples.bakehouse.media_gold_reviews_chunked")
# display(media_gold_reviews_chunked_df)

media_customer_reviews_df = spark.table("samples.bakehouse.media_customer_reviews")
# display(media_customer_reviews_df)

# Using EXPLICIT JOIN CONDITIONS - break chain to avoid ambiguous column references
temp_df1 = sales_transactions_df.join(
    sales_customers_df, 
    sales_transactions_df.customerID == sales_customers_df.customerID, 
    how="left"
)

temp_df2 = temp_df1.join(
    sales_franchises_df,
    on="franchiseID",
    how="left"
)

denormalized_sales_transactions_df = temp_df2.join(
    sales_suppliers_df,
    temp_df2.supplierID == sales_suppliers_df.supplierID,
    how="left"
)

temp_df3 = denormalized_sales_transactions_df.join(
    media_gold_reviews_chunked_df,
    on="franchiseID",
    how="left"
)

denormalized_df = temp_df3.join(
    media_customer_reviews_df,
    on="franchiseID",
    how="left"
)

display(denormalized_df.limit(10))

filtered_df = denormalized_df.filter(col("franchise_name").isNotNull())
display(filtered_df)
