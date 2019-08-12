import org.apache.spark.sql.SparkSession

val ss = SparkSession.builder
  .appName("Basic")
  .master("local[4]")
  .getOrCreate()

val fd2015 = ss
  .read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv("/Users/anup.sethuram/DEV/MGIT/de-spark/src/resources/2015-summary.csv")

println("Take 3")
fd2015.take(3)

ss.conf.set("spark.sql.shuffle.partitions", "10")
fd2015.sort("count").take(2)


println("Create View")
fd2015.createOrReplaceTempView("flight_data_2015")

print("Count: ")
fd2015.count()

fd2015.groupBy("DEST_COUNTRY_NAME").count().take(10)




