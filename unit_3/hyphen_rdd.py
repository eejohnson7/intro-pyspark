'''
Building on what you've learned about map transformations, let's put your skills to work.

In this task, you'll experience PySpark's power by transforming username strings to replace underscores with dashes. Follow these steps:

Apply a map transformation using the lambda function:
lambda username: username.replace('_', '-')

Retrieve and print the transformed results.
Watch as those usernames transform! This hands-on experience will enhance your mastery.
'''

from pyspark.sql import SparkSession

# Initialize SparkSession for transformation operations
spark = SparkSession.builder \
    .master("local") \
    .appName("MapTransformation") \
    .getOrCreate()

# Create an RDD with username strings containing underscores
rdd = spark.sparkContext.parallelize(["john_doe", "jane_smith", "alice_wonderland", "bob_builder", "charlie_brown"])

# TODO: Apply a map transformation to replace underscores with dashes in each username
mapped_rdd = rdd.map(lambda username: username.replace('_', '-'))


# TODO: Retrieve and print the transformed usernames from the RDD
print("Transformed elements:", mapped_rdd.collect())

# Stop SparkSession to release resources
spark.stop()
