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

fd2015.groupBy("DEST_COUNTRY_NAME").count().take(100)

import org.apache.spark.sql.functions.max
fd2015.select(max("count")).take(1)

fd2015.groupBy("ORIGIN_COUNTRY_NAME").max("count").limit(5).collect()

// TOP FIVE DESTINATION COUNTRIES.
//ss.sql(
//  """
//    |SELECT DEST_COUNTRY_NAME, sum(COUNT) as dest_total
//    |FROM flight_data_2015
//    |GROUP BY DEST_COUNTRY_NAME
//    |ORDER BY sum(COUNT) DESC
//    |LIMIT 5
//    |""".stripMargin).collect()

val maxSql = ss.sql("SELECT DEST_COUNTRY_NAME, sum(count) as destination_total FROM flight_data_2015 GROUP BY DEST_COUNTRY_NAME ORDER BY sum(count) DESC LIMIT 5")

maxSql.collect()