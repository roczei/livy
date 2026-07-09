"""
Exercises delegation token renewal: repeatedly accesses services while sleeping longer
than the configured delegation token max lifetime. Fails if tokens expire without renewal.
"""
import time

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("batch-dt-renewal").getOrCreate()
sc = spark.sparkContext

services = set(
    (spark.conf.get("spark.livy.test.services", "hdfs") or "hdfs").split(",")
)
services = {s.strip().lower() for s in services if s.strip()}

sleep_seconds = int(spark.conf.get("spark.livy.test.renewal.sleep.seconds", "45"))
check_interval = int(spark.conf.get("spark.livy.test.renewal.check.interval.seconds", "8"))


def access_hdfs():
    path = spark.conf.get("spark.livy.test.hdfs.path", "/tmp")
    scratch = path.rstrip("/") + "/renewal-" + str(int(time.time() * 1000))
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(scratch),
        sc._jsc.hadoopConfiguration())
    p = sc._jvm.org.apache.hadoop.fs.Path(scratch)
    assert fs.mkdirs(p) and fs.exists(p)


def access_ozone():
    base = spark.conf.get("spark.livy.test.ozone.path")
    assert base is not None and base.startswith(("ofs://", "o3fs://"))
    scratch = base.rstrip("/") + "/renewal-" + str(int(time.time() * 1000))
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(scratch),
        sc._jsc.hadoopConfiguration())
    p = sc._jvm.org.apache.hadoop.fs.Path(scratch)
    assert fs.mkdirs(p) and fs.exists(p)


def access_hive():
    count = spark.sql("SHOW DATABASES").count()
    assert count > 0


def access_hbase():
    conf = sc._jsc.hadoopConfiguration()
    hbase_conf = sc._jvm.org.apache.hadoop.hbase.HBaseConfiguration.create(conf)
    conn = sc._jvm.org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(
        hbase_conf)
    try:
        admin = conn.getAdmin()
        admin.listNamespaceDescriptors()
    finally:
        conn.close()


def access_kafka():
    bootstrap = spark.conf.get("kafka.bootstrap.servers")
    assert bootstrap is not None and len(bootstrap) > 0
    props = sc._jvm.java.util.Properties()
    props.put("bootstrap.servers", bootstrap)
    consumer = sc._jvm.org.apache.kafka.clients.consumer.KafkaConsumer(props)
    consumer.close()


ACCESSORS = {
    "hdfs": access_hdfs,
    "ozone": access_ozone,
    "hive": access_hive,
    "hbase": access_hbase,
    "kafka": access_kafka,
}

deadline = time.time() + sleep_seconds
checks = 0
while time.time() < deadline:
    for svc in sorted(services):
        ACCESSORS[svc]()
    checks += 1
    remaining = deadline - time.time()
    if remaining > 0:
        time.sleep(min(check_interval, remaining))

assert checks >= 2, "expected multiple renewal windows, got %d checks" % checks
spark.stop()
