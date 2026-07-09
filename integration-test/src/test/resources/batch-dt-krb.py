from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("batch-dt-krb").getOrCreate()
count = spark.sparkContext.parallelize(range(100)).count()
assert count == 100
spark.stop()
