'''
Well done on grasping map transformations in PySpark. Let's take it a step further with a small tweak that affects data 
processing.

In this task, you'll modify the lambda function inside the map transformation to cube each element instead of squaring. 
The steps are simple:

Change the operation to cube each element (i.e., x ** 3).
Rename the RDD from squared_rdd to cubed_rdd.
Update the print statement to reflect cubed elements.
You'll see firsthand how changes in transformations impact your results.
'''

from pyspark.sql import SparkSession

# Initialize SparkSession for transformation operations
spark = SparkSession.builder \
    .master("local") \
    .appName("MapTransformation") \
    .getOrCreate()

# Create an RDD as the basis for transformation
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# TODO: Change the lambda function to cube each element in the RDD
cubed_rdd = rdd.map(lambda x: x ** 3)

# TODO: Update the print statement to reflect cubed elements
print("Elements cubed in the RDD:", cubed_rdd.collect())

# Stop SparkSession to release resources
spark.stop()