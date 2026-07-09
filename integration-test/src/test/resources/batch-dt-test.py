from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("batch-dt-test").getOrCreate()
assert spark.sparkContext.parallelize(range(10)).count() == 10
spark.stop()
