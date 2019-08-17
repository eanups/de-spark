
import org.apache.spark.sql.{Row, SparkSession}

import scala.io.Source

object Basic extends App {

  val spark = SparkSession.builder
    .appName("Basic")
    .master("local[4]")
    .getOrCreate()


  val fd2015 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/resources/2015-summary.csv")


  println("Take 3")
  println(fd2015.take(3))

  spark.conf.set("spark.sql.shuffle.partitions", "10")
  fd2015.sort("count").take(2)


  println("** CREATE FLIGHT DATA VIEW ** ")
  fd2015.createOrReplaceTempView("flight_data_2015")

  val fd_sql = spark.sql(
    """
      SELECT DEST_COUNTRY_NAME, count(1)
      FROM flight_data_2015
      GROUP BY DEST_COUNTRY_NAME
      """)

  val fd_df = fd2015.groupBy("DEST_COUNTRY_NAME").count()

  println("SQL explanation: ", fd_sql.explain())
  println("DF explanation: ", fd_df.explain())


  fd_sql.printSchema()
  for (item <- fd_sql.take(100)) {
    println(item)
  }

  val maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

  for (item <- maxSql.collect()) {
    println(item)
  }
}


