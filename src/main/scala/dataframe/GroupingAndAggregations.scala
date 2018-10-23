package dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Dataset, Column}
import org.apache.spark.sql.functions._

/*
  Shows various forms of Aggregations and Groupings
*/

object GroupingAndAggregations {

  //Create a Case class of Orders
  case class Orders(order_id: Int, order_date: String, order_cust_id: Int, order_status: String)

  def main (args: Array[String] ): Unit = {

    val spark = SparkSession.builder().appName("Data Frame - GroupingAndAggregations").master("local[4]").getOrCreate()
    import spark.implicits._

    //Create a squence of case class objects
    val ord = Seq(
      Orders(6,"2013-07-25 00:00:00.0",7130,"COMPLETE"),
      Orders(7,"2013-07-25 00:00:00.0",4530,"COMPLETE"),
      Orders(8,"2013-07-25 00:00:00.0",2911,"PROCESSING"),
      Orders(9,"2013-07-25 00:00:00.0",5657,"PENDING_PAYMENT"),
      Orders(10,"2013-07-25 00:00:00.0",5648,"PENDING_PAYMENT"),
      Orders(11,"2013-07-25 00:00:00.0",918,"PAYMENT_REVIEW"),
      Orders(12,"2013-07-25 00:00:00.0",1837,"CLOSED"),
      Orders(13,"2013-07-25 00:00:00.0",9149,"PENDING_PAYMENT"),
      Orders(14,"2013-07-25 00:00:00.0",9842,"PROCESSING")
    )

    //Create an RDD and convert into a DataFrame
    val orderDF = spark.sparkContext.parallelize(ord).toDF("ord_id","ord_date","ord_cust_id","ord_status")
    orderDF.printSchema()
    orderDF.show()
    /*
      +------+--------------------+-----------+---------------+
      |ord_id|            ord_date|ord_cust_id|     ord_status|
      +------+--------------------+-----------+---------------+
      |     6|2013-07-25 00:00:...|       7130|       COMPLETE|
      |     7|2013-07-25 00:00:...|       4530|       COMPLETE|
      |     8|2013-07-25 00:00:...|       2911|     PROCESSING|
      |     9|2013-07-25 00:00:...|       5657|PENDING_PAYMENT|
      |    10|2013-07-25 00:00:...|       5648|PENDING_PAYMENT|
      |    11|2013-07-25 00:00:...|        918| PAYMENT_REVIEW|
      |    12|2013-07-25 00:00:...|       1837|         CLOSED|
      |    13|2013-07-25 00:00:...|       9149|PENDING_PAYMENT|
      |    14|2013-07-25 00:00:...|       9842|     PROCESSING|
      +------+--------------------+-----------+---------------+
     */

    /*
    Difference between grouping & Aggregations:
    1. Grouping:
    a. It does grouping rows and doesn't do anything beyond this
    b. You can't even print the data

    2. Aggregations:
    a. An aggregation is an function, which you pass each row of non-grouped column that you want to aggregate.
     */

    //Basic form of aggregations
    orderDF.groupBy($"ord_status").agg("ord_cust_id" -> "max").show()
    /*
    1. Below Grouped by per Order status
    2. returned Max Customer_Id per order status.
      +---------------+----------------+
      |     ord_status|max(ord_cust_id)|
      +---------------+----------------+
      |PENDING_PAYMENT|            9149|
      |       COMPLETE|            7130|
      | PAYMENT_REVIEW|             918|
      |     PROCESSING|            9842|
      |         CLOSED|            1837|
      +---------------+----------------+
     */

    //The other way to apply aggregate functions is by using sql.functions
    orderDF.groupBy($"ord_status").agg(max($"ord_cust_id")).show()
    /*
    +---------------+----------------+
    |     ord_status|max(ord_cust_id)|
    +---------------+----------------+
    |PENDING_PAYMENT|            9149|
    |       COMPLETE|            7130|
    | PAYMENT_REVIEW|             918|
    |     PROCESSING|            9842|
    |         CLOSED|            1837|
    +---------------+----------------+
     */

    //Column based aggregations and grouping columns
    //Fields are in Aggregate functions are to be listed in groupBy if no aggregate function is not specified.
    //field expression should be neither present in the group by, nor is it an aggregate function.
    // Add to group by or wrap in first() (or first_value) if you don't care which value you get.;;
    orderDF.groupBy($"ord_status").agg(max($"ord_cust_id"), min($"ord_cust_id"), avg($"ord_cust_id"), first($"ord_cust_id")).show()
    orderDF.groupBy($"ord_status").agg(max($"ord_cust_id"), min($"ord_cust_id"), avg($"ord_cust_id")).show()

    //Write a user-defined Agg function and apply
    def stddevFunc(c: Column): Column = {
      sqrt(c * c) - (avg(c) * avg(c))
    }

    //Apply above User-defined function
    orderDF.groupBy($"ord_status").agg($"ord_status", stddevFunc($"ord_cust_id")).show()
    orderDF.groupBy($"ord_status").count().show()



}

}
