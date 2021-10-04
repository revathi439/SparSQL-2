import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

object Assignment2 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("sparkSQL2")
    .getOrCreate()


  val data = Seq(Row("Banana", 1000, "USA"),
    Row("Carrots", 1500, "INDIA"),
    Row("Beans", 1600, "Sweden"),
    Row("Orange", 2000, "UK"),
    Row("Orange", 2000, "UAE"),
    Row("Banana", 400, "China"),
    Row("Carrots", 1200, "China"))
  val schema = Service.newSchema()
  val data1 = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  data1.printSchema()
  data1.show()

  //2.Find total amount exported to each country of each product.
 val pivotDataFrame= Service.data_pivot(data1)
   pivotDataFrame.show()


//3.Perform unpivot function on output of question 2.
val Country_names = "stack(7, 'UK',UK,'UAE',UAE,'USA',USA,'Sweden', Sweden, 'China', China, 'USA', USA, 'INDIA', INDIA) as (Country,Total)"
  Service.data_unpivot(pivotDataFrame,Country_names).show()

}
