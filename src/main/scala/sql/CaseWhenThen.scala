package sql

import org.apache.spark.sql.SparkSession

object CaseWhenThen {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark-CaseWhenThen").master("local[4]").getOrCreate()
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

// =====================================================================================================================================================================

    orders.createOrReplaceTempView("orders")




  }
}
