# datalake-test


```
data.write.format("delta").save("/tmp/delta-table")

data.write.format("delta").save("/Users/oketo/Downloads/data-lake-test/dale-tmp/delta-table")

(base) lasalles-MacBook-Pro:data oketo$ java -XshowSettings:properties -version 2>&1 > /dev/null | grep 'java.home' 
    java.home = /Library/Java/JavaVirtualMachines/jdk-12.0.1.jdk/Contents/Home



pyspark --packages io.delta:delta-spark_2.12:3.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

https://docs.delta.io/latest/quick-start.html

data = spark.range(0, 5)
data.write.format("delta").save("/Users/oketo/Downloads/data-lake-test/dale-tmp/delta-table")

>>> data.printSchema()
root
 |-- id: long (nullable = false)

>>> data.show()
+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
+---+

nyc = spark.read.parquet("/Users/oketo/Downloads/data-lake-test/data/*parquet")


>>> nyc.count()
5980721
>>> nyc.printSchema()
root
 |-- VendorID: long (nullable = true)
 |-- tpep_pickup_datetime: timestamp_ntz (nullable = true)
 |-- tpep_dropoff_datetime: timestamp_ntz (nullable = true)
 |-- passenger_count: double (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- RatecodeID: double (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: long (nullable = true)
 |-- DOLocationID: long (nullable = true)
 |-- payment_type: long (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- congestion_surcharge: double (nullable = true)
 |-- airport_fee: double (nullable = true)

>>> 

nyc.write.format("delta").save("/Users/oketo/Downloads/data-lake-test/dale-tmp/nyc-yellowcab-tbl")


data = spark.range(0, 5)
data.write.mode("overwrite").format("delta").save("/Users/oketo/Downloads/data-lake-test/dale-tmp/delta-table")

pyspark --packages io.delta:delta-spark_2.12:3.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"


df = spark.read.format("delta").load("/Users/oketo/Downloads/data-lake-test/dale-tmp/delta-table")
df.show()

data = spark.range(5, 10)
data.write.format("delta").mode("overwrite").save("/Users/oketo/Downloads/data-lake-test/dale-tmp/delta-table")

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, "/Users/oketo/Downloads/data-lake-test/dale-tmp/delta-table")

# Update every even value by adding 100 to it
deltaTable.update(
  condition = expr("id % 2 == 0"),
  set = { "id": expr("id + 100") })

# Delete every even value
deltaTable.delete(condition = expr("id % 2 == 0"))

# Upsert (merge) new data
newData = spark.range(0, 20)

deltaTable.alias("oldData") \
  .merge(
    newData.alias("newData"),
    "oldData.id = newData.id") \
  .whenMatchedUpdate(set = { "id": col("newData.id") }) \
  .whenNotMatchedInsert(values = { "id": col("newData.id") }) \
  .execute()

deltaTable.toDF().show()

df = spark.read.format("delta").option("versionAsOf", 0).load("/Users/oketo/Downloads/data-lake-test/dale-tmp/delta-table")
df.show()

df = spark.read.format("delta").option("versionAsOf", 1).load("/Users/oketo/Downloads/data-lake-test/dale-tmp/delta-table")
df.show()

df = spark.read.format("delta").option("versionAsOf", 4).load("/Users/oketo/Downloads/data-lake-test/dale-tmp/delta-table")
df.show()

```
