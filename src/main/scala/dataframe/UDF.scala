package dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UDF {

  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrame - UDF").master("local[4]").getOrCreate()
    import spark.implicits._

    //create RDD of case objects
    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )

    val customerDF = spark.sparkContext.parallelize(custs, 4).toDF()

    val mySales = udf{
      (sales: Double, disc: Double) => sales - disc
    }


    //Apply UDF on Select
    customerDF.select($"id", $"name", $"state", mySales($"sales", $"discount").as("After Discount")).show()
    /*
    +---+---------------+-----+--------------+
    | id|           name|state|After Discount|
    +---+---------------+-----+--------------+
    |  1|      Widget Co|   AZ|      120000.0|
    |  2|   Acme Widgets|   CA|      410000.0|
    |  3|       Widgetry|   CA|      410300.0|
    |  4|   Widgets R Us|   CA|      410500.0|
    |  5|Ye Olde Widgete|   MA|         500.0|
    +---+---------------+-----+--------------+
     */


    //UDF filter
    val myNameFilter = udf{
      (s: String) => s.startsWith("W")
    }

    //Apply filter UDF function
    customerDF.filter(myNameFilter($"name")).show()
    /*
    +---+------------+--------+--------+-----+
    | id|        name|   sales|discount|state|
    +---+------------+--------+--------+-----+
    |  1|   Widget Co|120000.0|     0.0|   AZ|
    |  3|    Widgetry|410500.0|   200.0|   CA|
    |  4|Widgets R Us|410500.0|     0.0|   CA|
    +---+------------+--------+--------+-----+
     */

    //UDF grouping
    def stateRegion = udf {
      (state: String) => state match {
      case "CA" | "AK" | "OR" | "WA" => "West"
      case "ME" | "NH" | "MA" | "RI" | "CT" | "VT" => "North"
      case "AZ" | "NM" | "CO" | "UT" => "SouthWest"
    }
    }

    customerDF.groupBy(stateRegion($"state").as("Region")).count().show()
    /*
    +---------+-----+
    |   Region|count|
    +---------+-----+
    |SouthWest|    1|
    |     West|    3|
    |    North|    1|
    +---------+-----+
     */

  }
}
