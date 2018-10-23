package dataframe

import org.apache.spark.sql.SparkSession

object DropDuplicates {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrame-Drop Duplicates").master("local[4]").getOrCreate()
    import spark.implicits._

    //Create RDD of duplicates
    val ord = Seq(
      (1,"2013-07-25 00:00:00.0",11599,"CLOSED"),
      (2,"2013-07-25 00:00:00.0",256,"PENDING_PAYMENT"),
      (3,"2013-07-25 00:00:00.0",256,"PENDING_PAYMENT"),
      (4,"2013-07-25 00:00:00.0",8827,"CLOSED"),
      (4,"2013-07-25 00:00:00.0",8827,"CLOSED"),
      (6,"2013-07-25 00:00:00.0",11318,"COMPLETE"),
      (6,"2013-07-25 00:00:00.0",11318,"COMPLETE")
    )

    //Create an RDD
    val orderDF = spark.sparkContext.parallelize(ord, 4).toDF("orderId","order_date","order_cust_id","order_status")

    //Print Schema
    orderDF.printSchema()
    orderDF.show()
    /*
    +-------+--------------------+-------------+---------------+
    |orderId|          order_date|order_cust_id|   order_status|
    +-------+--------------------+-------------+---------------+
    |      1|2013-07-25 00:00:...|        11599|         CLOSED|
    |      2|2013-07-25 00:00:...|          256|PENDING_PAYMENT|
    |      3|2013-07-25 00:00:...|          256|PENDING_PAYMENT|
    |      4|2013-07-25 00:00:...|         8827|         CLOSED|
    |      4|2013-07-25 00:00:...|         8827|         CLOSED|
    |      6|2013-07-25 00:00:...|        11318|       COMPLETE|
    |      6|2013-07-25 00:00:...|        11318|       COMPLETE|
    +-------+--------------------+-------------+---------------+
     */

    //Drop identical records ==>> Identify duplicates based on OrderID by default unless you specify columns
    val withoutDuplicates = orderDF.dropDuplicates()
    withoutDuplicates.show()
    /*
    +-------+--------------------+-------------+---------------+
    |orderId|          order_date|order_cust_id|   order_status|
    +-------+--------------------+-------------+---------------+
    |      1|2013-07-25 00:00:...|        11599|         CLOSED|
    |      6|2013-07-25 00:00:...|        11318|       COMPLETE|
    |      4|2013-07-25 00:00:...|         8827|         CLOSED|
    |      2|2013-07-25 00:00:...|          256|PENDING_PAYMENT|
    |      3|2013-07-25 00:00:...|          256|PENDING_PAYMENT|
    +-------+--------------------+-------------+---------------+
     */

    //drop duplicates based on columns
    val withoutPartials = withoutDuplicates.dropDuplicates(Seq("order_cust_id", "order_status"))
    withoutPartials.show()
    /*
    +-------+--------------------+-------------+---------------+
    |orderId|          order_date|order_cust_id|   order_status|
    +-------+--------------------+-------------+---------------+
    |      4|2013-07-25 00:00:...|         8827|         CLOSED|
    |      1|2013-07-25 00:00:...|        11599|         CLOSED|
    |      6|2013-07-25 00:00:...|        11318|       COMPLETE|
    |      2|2013-07-25 00:00:...|          256|PENDING_PAYMENT|
    +-------+--------------------+-------------+---------------+
     */
  }
}
