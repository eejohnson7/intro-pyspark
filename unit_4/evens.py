'''
Building on what you've learned about filtering RDD elements, let's put it into practice.

In this exercise, your task is to complete a code snippet that filters even numbers from a list using a filter transformation 
method in PySpark.

Keep up the momentum, and you'll soon master these techniques!
'''

from pyspark.sql import SparkSession

# Initialize SparkSession to perform filter operations
spark = SparkSession.builder \
    .master("local") \
    .appName("FilterTransformation") \
    .getOrCreate()

# Create an RDD for applying filter transformations
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# TODO: Filter the RDD to retain only even numbers using a lambda function
even_rdd = rdd.filter(lambda x: x % 2 == 0)

# Retrieve and print the filtered elements from the RDD
print("Even elements in the RDD:", even_rdd.collect())

# Stop SparkSession to release resources
spark.stop()