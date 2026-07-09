from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("batch-dt-services").getOrCreate()
sc = spark.sparkContext

services = set(
    (spark.conf.get("spark.livy.test.services", "hdfs") or "hdfs").split(",")
)
services = {s.strip().lower() for s in services if s.strip()}

if "hdfs" in services:
    path = spark.conf.get("spark.livy.test.hdfs.path", "/tmp")
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(path),
        sc._jsc.hadoopConfiguration())
    fs.mkdirs(sc._jvm.org.apache.hadoop.fs.Path(path))
    assert fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))

if "ozone" in services:
    path = spark.conf.get("spark.livy.test.ozone.path")
    assert path is not None and path.startswith(("ofs://", "o3fs://"))
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(path),
        sc._jsc.hadoopConfiguration())
    scratch = path.rstrip("/") + "/dt-it-" + str(sc._jvm.java.lang.System.currentTimeMillis())
    fs.mkdirs(sc._jvm.org.apache.hadoop.fs.Path(scratch))
    assert fs.exists(sc._jvm.org.apache.hadoop.fs.Path(scratch))

if "hive" in services:
    count = spark.sql("SHOW DATABASES").count()
    assert count > 0

if "hbase" in services:
    conf = sc._jsc.hadoopConfiguration()
    hbase_conf = sc._jvm.org.apache.hadoop.hbase.HBaseConfiguration.create(conf)
    conn = sc._jvm.org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(
        hbase_conf)
    try:
        admin = conn.getAdmin()
        admin.listNamespaceDescriptors()
    finally:
        conn.close()

if "kafka" in services:
    bootstrap = spark.conf.get("kafka.bootstrap.servers")
    assert bootstrap is not None and len(bootstrap) > 0
    props = sc._jvm.java.util.Properties()
    props.put("bootstrap.servers", bootstrap)
    consumer = sc._jvm.org.apache.kafka.clients.consumer.KafkaConsumer(props)
    consumer.close()

spark.stop()
