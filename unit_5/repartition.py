'''
In this task, you'll deepen your understanding of how RDDs are partitioned and saved.

Your objective is to use the repartition function to divide the product_names_rdd RDD into 5 partitions before saving it. 
Here's a generic example of how to use it:

# Repartition the RDD into 3 partitions
partitioned_rdd = my_rdd.repartition(3)

After partitioning, the RDD will be saved as a series of part files, with each partition corresponding to a separate file. 
This will help you understand the partitioning process and see its impact by observing the number of files created in the 
output_products directory.
'''

import os
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .master("local") \
    .appName("SalesDataProcessing") \
    .getOrCreate()

# Create an RDD by reading from a sales data file
sales_rdd = spark.sparkContext.textFile("sales.txt")

# Use filter to retain only sales entries with amounts greater than $100
high_value_sales_rdd = sales_rdd.filter(lambda line: float(line.split(",")[1]) > 100)

# Use map to extract product names from high value sales, assuming format: "Product,Amount"
product_names_rdd = high_value_sales_rdd.map(lambda line: line.split(",")[0])

# TODO: Repartition the RDD into 5 partitions
partioned_rdd = product_names_rdd.repartition(5)

# Save the extracted product names to a new text file
partioned_rdd.saveAsTextFile("output_products")

# List the files generated in the output directory
output_files = os.listdir("output_products")
print("List of files generated:")
for file in output_files:
    if not os.path.splitext(file)[1]:
        print(file)

# Stop SparkSession to release resources
spark.stop()