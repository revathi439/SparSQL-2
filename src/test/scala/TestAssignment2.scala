import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class TestAssignment2 extends AnyFunSuite {

  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("SparkSQL2")
    .getOrCreate()

  //Solution1
  val data = Seq(Row("Carrot", 1000, "India"),
    Row("Apple", 1500, "India"),
    Row("Carrot", 1000, "London"),
    Row("Apple", 900, "London"),
    Row("Pineapple",2000,"India"),
    Row("Pineapple",1500,"London"))
  val schema = Service.newSchema()
  val dataF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

  //Solution2
  import spark.implicits._
  val pivotDF = Service.data_pivot(dataF)
  assert(pivotDF.collect().toList === List(("Pineapple",2000,1500),("Carrot",1000,1000),("Apple",1500,900)).toDF.collect.toList)

  // Solution 3
  val Country_names: String ="stack(2, 'India', India, 'London', London) as (Country, Total)"
  assert(Service.data_unpivot(pivotDF,Country_names).collect().toList===List(("Pineapple","India",2000),("Pineapple","London",1500),("Carrot","India",1000),("Carrot","London",1000),("Apple","India",1500),("Apple","London",900)).toDF.collect().toList)


}