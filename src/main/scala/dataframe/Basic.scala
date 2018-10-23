package dataframe

import org.apache.spark.sql.SparkSession

/*
  Create a DataFrame from an RDD of a case class object and perform some basic dataframe operations
  such as Show, Select, filter.
 */


object Basic {

  case class Order(ord_id: Int, ord_date: String, ord_cust_id: Double, ord_status: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[4]").appName("DataFrame-Basic").getOrCreate()
    import spark.implicits._

    //Create records in a  Sequence for a case class
    val ord = Seq(
      Order(1,"2013-07-25 00:00:00.0",11599,"CLOSED"),
      Order(2,"2013-07-25 00:00:00.0",256,"PENDING_PAYMENT"),
      Order(3,"2013-07-25 00:00:00.0",12111,"COMPLETE"),
      Order(4,"2013-07-25 00:00:00.0",8827,"CLOSED"),
      Order(5,"2013-07-25 00:00:00.0",11318,"COMPLETE")
    )

    //Create Ord into an RDD and create DataFrame
    val ordDF = spark.sparkContext.parallelize(ord).toDF()

    //View the Schema of DataFrame
    ordDF.toString()
    ordDF.printSchema()

    //View records from DataFrame. by default it shows only first 20 records.
    ordDF.show()

    //Select single field
    ordDF.select($"ord_id").show()

    //Select multiple fields
    ordDF.select($"ord_id", $"ord_cust_id", $"ord_status").show()

    //Filter data
    ordDF.filter($"ord_status".equalTo("CLOSED")).show()
  }

}
