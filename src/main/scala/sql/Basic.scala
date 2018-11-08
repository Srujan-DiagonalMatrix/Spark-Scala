package sql

import org.apache.spark.sql.SparkSession

object Basic {
  def main(args: Array[String]): Unit = {

  /*
    - Here we'll be using Orders and order_items from Retail_DB
    - Create Case class objects and load data into it.
    - Convert to DataFrames and query them.
    - Show the results.
  */

    //Instantiate Spark Application
    val spark = SparkSession.builder().appName("Spark-Basic").master("local[4]").getOrCreate()
    import spark.implicits._

    //Create Case object for Orders and Order items
    case class order(order_id: Int, order_date: String, order_cust_id: Int, order_status: String)
    case class order_items(order_itm_id: Int, order_itm_ord_id: Int, order_itm_prod_id: Int, order_itm_qty: Double, order_itm_sub_total: Double, order_itm_prod_price: Double)

    /*
      - Create RDD from order/order_item text files and convert to case class objects
      -  Create Dataframe and perform join
    */
    val ordRDD = spark.sparkContext.textFile("/Users/Alikanti/Research/data/retail_db/orders/part-00000")
    val ordItmRDD = spark.sparkContext.textFile("/Users/Alikanti/Research/data/retail_db/order_items/part-00000")

    val ordDF = ordRDD.map(ele => ele.split(",")).map(rowVal => order(rowVal(0).toInt,rowVal(1).toString,rowVal(2).toInt,rowVal(3).toString)).toDF()
    val ordItmDF = ordItmRDD.map(ele => ele.split(",")).map(rowVal => order_items(rowVal(0).toInt,rowVal(1).toInt,rowVal(2).toInt,rowVal(3).toDouble,rowVal(4).toDouble,rowVal(5).toDouble)).toDF()

    val orders = ordDF.join(ordItmDF).where(ordDF.col("order_id").equalTo(ordItmDF.col("order_itm_ord_id")))
    orders.show()
    /*
--------+--------------------+-------------+---------------+------------+----------------+-----------------+-------------+-------------------+--------------------+
|order_id|          order_date|order_cust_id|   order_status|order_itm_id|order_itm_ord_id|order_itm_prod_id|order_itm_qty|order_itm_sub_total|order_itm_prod_price|
+--------+--------------------+-------------+---------------+------------+----------------+-----------------+-------------+-------------------+--------------------+
|     148|2013-07-26 00:00:...|         5383|     PROCESSING|         348|             148|              502|          2.0|              100.0|                50.0|
|     148|2013-07-26 00:00:...|         5383|     PROCESSING|         349|             148|              502|          5.0|              250.0|                50.0|
|     148|2013-07-26 00:00:...|         5383|     PROCESSING|         350|             148|              403|          1.0|             129.99|              129.99|
|     463|2013-07-27 00:00:...|         8709|       COMPLETE|        1129|             463|              365|          4.0|             239.96|               59.99|
|     463|2013-07-27 00:00:...|         8709|       COMPLETE|        1130|             463|              502|          5.0|              250.0|                50.0|
|     463|2013-07-27 00:00:...|         8709|       COMPLETE|        1131|             463|              627|          1.0|              39.99|               39.99|
|     463|2013-07-27 00:00:...|         8709|       COMPLETE|        1132|             463|              191|          3.0|             299.97|               99.99|
|     471|2013-07-27 00:00:...|        10861|        PENDING|        1153|             471|              627|          1.0|              39.99|               39.99|
|     471|2013-07-27 00:00:...|        10861|        PENDING|        1154|             471|              403|          1.0|             129.99|              129.99|
|     496|2013-07-27 00:00:...|          615|       COMPLETE|        1223|             496|              365|          1.0|              59.99|               59.99|
|     496|2013-07-27 00:00:...|          615|       COMPLETE|        1224|             496|              502|          3.0|              150.0|                50.0|
|     496|2013-07-27 00:00:...|          615|       COMPLETE|        1225|             496|              821|          1.0|              51.99|               51.99|
|     496|2013-07-27 00:00:...|          615|       COMPLETE|        1226|             496|              403|          1.0|             129.99|              129.99|
|     496|2013-07-27 00:00:...|          615|       COMPLETE|        1227|             496|             1014|          1.0|              49.98|               49.98|
|    1088|2013-07-31 00:00:...|        11139|PENDING_PAYMENT|        2703|            1088|              403|          1.0|             129.99|              129.99|
|    1088|2013-07-31 00:00:...|        11139|PENDING_PAYMENT|        2704|            1088|              365|          2.0|             119.98|               59.99|
|    1580|2013-08-02 00:00:...|         9713|     PROCESSING|        3944|            1580|               44|          5.0|             299.95|               59.99|
|    1591|2013-08-02 00:00:...|        10666|       COMPLETE|        3968|            1591|              627|          5.0|             199.95|               39.99|
|    1591|2013-08-02 00:00:...|        10666|       COMPLETE|        3969|            1591|              627|          1.0|              39.99|               39.99|
|    1591|2013-08-02 00:00:...|        10666|       COMPLETE|        3970|            1591|             1014|          4.0|             199.92|               49.98|
+--------+--------------------+-------------+---------------+------------+----------------+-----------------+-------------+-------------------+--------------------+
    */






  }
}
