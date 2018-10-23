package dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Select {

  case class orders(ord_id: Int, ord_date: String, ord_cust_id: Int, ord_status: String)
  case class ord_disc(ordAmt: Double, discount: Double)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrame-Select").master("local[4]").getOrCreate()
    import spark.implicits._

    //Create case objects and create RDD
    val ord = Seq(
      orders(1,"2013-07-25 00:00:00.0",11599,"CLOSED"),
      orders(2,"2013-07-25 00:00:00.0",256,"PENDING_PAYMENT"),
      orders(3,"2013-07-25 00:00:00.0",12111,"COMPLETE"),
      orders(4,"2013-07-25 00:00:00.0",8827,"CLOSED"),
      orders(5,"2013-07-25 00:00:00.0",11318,"COMPLETE"),
      orders(6,"2013-07-25 00:00:00.0",7130,"COMPLETE"),
      orders(7,"2013-07-25 00:00:00.0",4530,"COMPLETE")
    )

    val ordDisc = Seq(
      ord_disc(1000, 10),
      ord_disc(2000, 11),
      ord_disc(3000, 12),
      ord_disc(4000, 13),
      ord_disc(5000, 14)
    )

    val ordDiscDF = spark.sparkContext.parallelize(ordDisc).toDF()

    val ord_df = spark.sparkContext.parallelize(ord).toDF()
    ord_df.show()
    /*
    +------+--------------------+-----------+---------------+
    |ord_id|            ord_date|ord_cust_id|     ord_status|
    +------+--------------------+-----------+---------------+
    |     1|2013-07-25 00:00:...|      11599|         CLOSED|
    |     2|2013-07-25 00:00:...|        256|PENDING_PAYMENT|
    |     3|2013-07-25 00:00:...|      12111|       COMPLETE|
    |     4|2013-07-25 00:00:...|       8827|         CLOSED|
    |     5|2013-07-25 00:00:...|      11318|       COMPLETE|
    |     6|2013-07-25 00:00:...|       7130|       COMPLETE|
    |     7|2013-07-25 00:00:...|       4530|       COMPLETE|
    +------+--------------------+-----------+---------------+
     */

    //Select * to select all columns in DataFrame
    ord_df.select("*").show()

    //Select multiple columns in a DataFrame
    ord_df.select($"ord_id", $"ord_date", $"ord_cust_id").show()

    //Below approach is, using apply() method on DataFrame to create an object, and select through them.
    ord_df.select(ord_df("ord_id"), ord_df("ord_date"), ord_df("ord_cust_id")).show()

    //column renaming
    ord_df.select(ord_df("ord_id").as("Order ID"), ord_df("ord_date").as("order date")).show()
    /*
    +--------+--------------------+
    |Order ID|          order date|
    +--------+--------------------+
    |       1|2013-07-25 00:00:...|
    |       2|2013-07-25 00:00:...|
    |       3|2013-07-25 00:00:...|
    |       4|2013-07-25 00:00:...|
    |       5|2013-07-25 00:00:...|
    |       6|2013-07-25 00:00:...|
    |       7|2013-07-25 00:00:...|
    +--------+--------------------+
     */

    //** $ ** is the shorthand for obtaining column
    ord_df.select($"ord_id".as("Order Id")).show()

    //Manipulating column values
    ord_df.select(($"ord_id" + 2).as("Order Id"), ($"ord_date").as("Order Date")).show()
    /*
    +--------+--------------------+
    |Order Id|          Order Date|
    +--------+--------------------+
    |       3|2013-07-25 00:00:...|
    |       4|2013-07-25 00:00:...|
    |       5|2013-07-25 00:00:...|
    |       6|2013-07-25 00:00:...|
    |       7|2013-07-25 00:00:...|
    |       8|2013-07-25 00:00:...|
    |       9|2013-07-25 00:00:...|
    +--------+--------------------+
     */

    ordDiscDF.select(($"ordAmt"-$"discount").as("Net Amount")).show()
    /*
    +----------+
    |Net Amount|
    +----------+
    |     990.0|
    |    1989.0|
    |    2988.0|
    |    3987.0|
    |    4986.0|
    +----------+
     */

    ord_df.select(ord_df("*"), $"ord_id".as("New ID")).show()
    /*
    +------+--------------------+-----------+---------------+------+
    |ord_id|            ord_date|ord_cust_id|     ord_status|New ID|
    +------+--------------------+-----------+---------------+------+
    |     1|2013-07-25 00:00:...|      11599|         CLOSED|     1|
    |     2|2013-07-25 00:00:...|        256|PENDING_PAYMENT|     2|
    |     3|2013-07-25 00:00:...|      12111|       COMPLETE|     3|
    |     4|2013-07-25 00:00:...|       8827|         CLOSED|     4|
    |     5|2013-07-25 00:00:...|      11318|       COMPLETE|     5|
    |     6|2013-07-25 00:00:...|       7130|       COMPLETE|     6|
    |     7|2013-07-25 00:00:...|       4530|       COMPLETE|     7|
    +------+--------------------+-----------+---------------+------+
     */
  }
}
