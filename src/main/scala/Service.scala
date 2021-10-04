import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object Service {

  //1.Create a non-nested dataframe with product, amount and country fields.
def newSchema():StructType={
new StructType()
  .add("Product",StringType)
  .add("Amount",IntegerType)
  .add("Country",StringType)
}

  //2.Find total amount exported to each country of each product.
  def data_pivot(dataDF:DataFrame):DataFrame= {
    val pivotDF =dataDF
      pivotDF.groupBy("Product","Country")
        .sum("Amount")
        .groupBy("Product")
        .pivot("Country")
        .sum("sum(Amount)")
  }

  //3.Perform unpivot function on output of question 2.
  def data_unpivot(dataDF:DataFrame,Country_names:String)(implicit spark:SparkSession):DataFrame={
    import spark.implicits._
    val unpivotDF = dataDF
    unpivotDF.select($"Product",expr(Country_names))
      .where("Total is not null")

  }
}
