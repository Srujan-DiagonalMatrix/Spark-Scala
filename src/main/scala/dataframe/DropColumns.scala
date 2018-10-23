package dataframe

import org.apache.spark.sql.SparkSession

object DropColumns {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrame - Drop Columns").master("local[4]").getOrCreate()
    import spark.implicits._

    val ord_item = Seq(
      (1,1,957,1,299.98,299.98),
      (2,2,1073,1,199.99,199.99),
      (3,2,502,5,250.0,50.0),
      (4,2,403,1,129.99,129.99),
      (5,4,897,2,49.98,24.99)
    )

    val ord_item_rows = spark.sparkContext.parallelize(ord_item).toDF("ord_itm_id", "ord_itm_ord_id", "ord_itm_prd_id", "ord_itm_qty", "ord_itm_subtotal", "ord_itm_prd_price")
    ord_item_rows.printSchema()
    ord_item_rows.show()
    /*
    +----------+--------------+--------------+-----------+----------------+-----------------+
    |ord_itm_id|ord_itm_ord_id|ord_itm_prd_id|ord_itm_qty|ord_itm_subtotal|ord_itm_prd_price|
    +----------+--------------+--------------+-----------+----------------+-----------------+
    |         1|             1|           957|          1|          299.98|           299.98|
    |         2|             2|          1073|          1|          199.99|           199.99|
    |         3|             2|           502|          5|           250.0|             50.0|
    |         4|             2|           403|          1|          129.99|           129.99|
    |         5|             4|           897|          2|           49.98|            24.99|
    +----------+--------------+--------------+-----------+----------------+-----------------+
     */

    //Drop Ord_itm_id and ord_itm_qty
    ord_item_rows.drop("ord_itm_id").drop("ord_itm_qty").show()
    /*
    +--------------+--------------+----------------+-----------------+
    |ord_itm_ord_id|ord_itm_prd_id|ord_itm_subtotal|ord_itm_prd_price|
    +--------------+--------------+----------------+-----------------+
    |             1|           957|          299.98|           299.98|
    |             2|          1073|          199.99|           199.99|
    |             2|           502|           250.0|             50.0|
    |             2|           403|          129.99|           129.99|
    |             4|           897|           49.98|            24.99|
    +--------------+--------------+----------------+-----------------+
     */
  }
}
