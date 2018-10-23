package dataframe

import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.types._

/*
  Here we create a DataFrame from an RDD[ROW] and a synthetic schema.
*/

object FromRowsAndSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[4]").appName("DataFrame-FromRowsAndSchema").getOrCreate()
    import spark.implicits._

    //Create RDD of rows
    val ord = Seq(
      Row(1,"2013-07-25 00:00:00.0",11599,"CLOSED"),
      Row(2,"2013-07-25 00:00:00.0",256,"PENDING_PAYMENT"),
      Row(3,"2013-07-25 00:00:00.0",12111,"COMPLETE"),
      Row(4,"2013-07-25 00:00:00.0",8827,"CLOSED"),
      Row(5,"2013-07-25 00:00:00.0",11318,"COMPLETE")
    )

    //Create an RDD of rows
    val orderRows = spark.sparkContext.parallelize(ord, 4)

    //Define a schema
    val ordSchema = StructType(
      Seq(
        StructField("ord_id", IntegerType, false),
        StructField("ord_date", StringType, false),
        StructField("ord_cust_id", IntegerType, false),
        StructField("ord_status", StringType, false)
      )
    )

    //Create a DataFrame combining Schema and Rows of data
    val customerDF = spark.createDataFrame(orderRows, ordSchema)
    customerDF.printSchema()

    customerDF.show()

    /*
    +------+--------------------+-----------+---------------+
    |ord_id|            ord_date|ord_cust_id|     ord_status|
    +------+--------------------+-----------+---------------+
    |     1|2013-07-25 00:00:...|      11599|         CLOSED|
    |     2|2013-07-25 00:00:...|        256|PENDING_PAYMENT|
    |     3|2013-07-25 00:00:...|      12111|       COMPLETE|
    |     4|2013-07-25 00:00:...|       8827|         CLOSED|
    |     5|2013-07-25 00:00:...|      11318|       COMPLETE|
    +------+--------------------+-----------+---------------+
     */
  }
}
