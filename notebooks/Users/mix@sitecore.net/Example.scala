// Databricks notebook source
val df = spark.read.json("/databricks-datasets/samples/people/people.json")

// COMMAND ----------

df.show()

// COMMAND ----------

case class Person (name: String, age: Long)
val ds = spark.read.json("/databricks-datasets/samples/people/people.json").as[Person]

// COMMAND ----------

display(ds)

// COMMAND ----------

// define a case class that represents the device data.
case class DeviceIoTData (
  battery_level: Long,
  c02_level: Long,
  cca2: String,
  cca3: String,
  cn: String,
  device_id: Long,
  device_name: String,
  humidity: Long,
  ip: String,
  latitude: Double,
  longitude: Double,
  scale: String,
  temp: Long,
  timestamp: Long
)

// read the JSON file and create the Dataset from the ``case class`` DeviceIoTData
// ds is now a collection of JVM Scala objects DeviceIoTData
val ds = spark.read.json("/databricks-datasets/iot/iot_devices.json").as[DeviceIoTData]

// COMMAND ----------

// Using the standard Spark commands, take() and foreach(), print the first
// 10 rows of the Datasets.
ds.take(10).foreach(println(_))


// COMMAND ----------

val dsTemp = ds.filter(d => d.temp > 25).map(d => (d.temp, d.device_name, d.cca3))
display(dsTemp)

// COMMAND ----------

val dsAvgTmp = ds.filter(d => {d.temp > 25}).map(d => (d.temp, d.humidity, d.cca3)).groupBy($"_3").avg()
display(dsAvgTmp)

// COMMAND ----------

display(ds.select($"battery_level", $"c02_level", $"device_name").where($"battery_level" > 6).sort($"c02_level"))

// COMMAND ----------

ds.createOrReplaceTempView("iot_device_data")

// COMMAND ----------

// MAGIC %sql select cca3, count (distinct device_id) as device_id from iot_device_data group by cca3 order by device_id desc limit 100