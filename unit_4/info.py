'''
Awesome work on understanding how to filter RDD elements! Now, let's apply what you've learned to filter log data from a 
text file using PySpark.

Here's what you need to do:

Use the following lambda function with the filter() method to retain only log lines that include the keyword "INFO".
lambda x: "INFO" in x

Print the resulting filtered RDD elements.
Dive in and solidify your PySpark skills with this exercise!
'''

from pyspark.sql import SparkSession

# Initialize SparkSession to perform filter operations
spark = SparkSession.builder \
    .master("local") \
    .appName("FilterTransformation") \
    .getOrCreate()

# Read the text file into an RDD
rdd = spark.sparkContext.textFile("logs.txt")

# TODO: Filter the RDD to retain only log lines containing the keyword "INFO" using a lambda function
rdd_filtered = rdd.filter(lambda x: "INFO" in x)

# TODO: Retrieve and print the filtered elements from the RDD
print("Lines that include the keyword INFO:", rdd_filtered.collect())

# Stop SparkSession to release resources
spark.stop()