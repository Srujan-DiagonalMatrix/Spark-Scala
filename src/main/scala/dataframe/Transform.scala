package dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Transform {

  case class customer(id: Int, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrame - Transform").master("local[4]").getOrCreate()
    import spark.implicits._

    val cust = Seq(
      customer(1, "Widget Co", 120000.00, 0.00, "AZ"),
      customer(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      customer(3, "Widgetry", 410500.00, 200.00, "CA"),
      customer(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      customer(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )

    val custDF = spark.sparkContext.parallelize(cust).toDF()
    custDF.show()
    /*
    +---+---------------+--------+--------+-----+
    | id|           name|   sales|discount|state|
    +---+---------------+--------+--------+-----+
    |  1|      Widget Co|120000.0|     0.0|   AZ|
    |  2|   Acme Widgets|410500.0|   500.0|   CA|
    |  3|       Widgetry|410500.0|   200.0|   CA|
    |  4|   Widgets R Us|410500.0|     0.0|   CA|
    |  5|Ye Olde Widgete|   500.0|     0.0|   MA|
    +---+---------------+--------+--------+-----+
     */

    //a trival UDF
    val myFunc = udf{
      (x: Double) => x + x
    }

    //Apply UDF to 'Discount' column
    val colNames = custDF.columns
    colNames.foreach(println(_))
    /*
    id
    name
    sales
    discount
    state
    */

    //This will return matching columns of custDF and colNames
    val cols = colNames.map(cName => custDF.col(cName))
    val theColumn = custDF.col("discount")


    //If column names matches apply above UDF function and rename the column as 'Transformed'
    val mappedCols = cols.map(
      c => if(c.toString == theColumn.toString) myFunc(c).as("Transformed") else c
    )

    //Select to produce new columns
    val newDF = custDF.select(mappedCols: _*)
    newDF.show()

  /*
    +---+---------------+--------+-----------+-----+
    | id|           name|   sales|Transformed|state|
    +---+---------------+--------+-----------+-----+
    |  1|      Widget Co|120000.0|        0.0|   AZ|
    |  2|   Acme Widgets|410500.0|     1000.0|   CA|
    |  3|       Widgetry|410500.0|      400.0|   CA|
    |  4|   Widgets R Us|410500.0|        0.0|   CA|
    |  5|Ye Olde Widgete|   500.0|        0.0|   MA|
    +---+---------------+--------+-----------+-----+
  */
  }
}
