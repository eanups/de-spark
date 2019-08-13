import org.apache.spark.sql.{Row, SparkSession}

object Basic extends App {

  val spark = SparkSession.builder
    .appName("Basic")
    .master("local[4]")
    .getOrCreate()

  val fd2015 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/Users/anup.sethuram/DEV/MGIT/de-spark/src/resources/2015-summary.csv")

  println("Take 3")
  println(fd2015.take(3))

  spark.conf.set("spark.sql.shuffle.partitions", "10")
  fd2015.sort("count").take(2)


  println("Create View")
  fd2015.createOrReplaceTempView("flight_data_2015")

  val fd_sql = spark.sql(
    """
      SELECT DEST_COUNTRY_NAME, count(*)
      FROM flight_data_2015
      GROUP BY DEST_COUNTRY_NAME
      """)

  fd_sql.printSchema()
  for (item <- fd_sql.take(10)) {
    println(item)
  }
}


