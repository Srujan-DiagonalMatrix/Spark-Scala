package dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, DataFrame}

/*
  This exercise gives you the difference between DataFrame and Dataset.
  For more details on extensive Dataset usage pls refer to Dataset folder.

  Learning point: you can create DF in 2 ways
  a). create an RDD eg: Spark.sparkContext.parallelize(<List>).toDF
  b). create DataFrame directly eg: Spark.CreatedataFrame(<CaseObject>)
 */

object DatasetConversion {

  case class order(ord_id: Int,
                   ord_date: String,
                   ord_cust_id: Int,
                   ord_status: String)

  case class order_small(ord_id: Int,
                         ord_date: String,
                         ord_status: String)

  val spark = SparkSession.builder().master("local[4]").appName("Dataset").getOrCreate()
  import spark.implicits._

  //Create Sequence of case class objects.
  val ord = Seq(
    order(1,"2013-07-25 00:00:00.0",11599,"CLOSED"),
    order(2,"2013-07-25 00:00:00.0",256,"PENDING_PAYMENT"),
    order(3,"2013-07-25 00:00:00.0",12111,"COMPLETE"),
    order(4,"2013-07-25 00:00:00.0",8827,"CLOSED"),
    order(5,"2013-07-25 00:00:00.0",11318,"COMPLETE")
  )

  //Create DataFrame directly without creating RDD
  val ordDF = spark.createDataFrame(ord)

  //View/print Schema
  ordDF.printSchema() //Display in Hierarchy
  ordDF.schema        //Display items in StructField
  ordDF.toString()    //Display in String

  //Display first 20 records
  ordDF.show()

  //Select & filter data
  val smallerDF = ordDF.select($"ord_id", $"ord_date", $"ord_status").filter($"ord_status".equalTo("COMPLETE"))
  smallerDF.show()


  /*
    Convert DataFrame into a Dataset.
    Make sure the column names should be same.
    make sure order of columns should also be same
   */
  val ordr: Dataset[order_small] = smallerDF.as[order_small]
  ordr.printSchema()
  ordr.show()



  val verySmallDS: Dataset[Double] = smallerDF.select($"ord_id".as[Double])
  verySmallDS.printSchema()
  verySmallDS.show()


  /*
  Convert to tuple
   */
  val tupleDS: Dataset[(Double, String, String)] = smallerDF.select($"ord_id".as[Double], $"ord_status".as[String], $"ord_date".as[String])
  tupleDS.printSchema()
  tupleDS.show()


  /*
  revert back from tuple to Dataset, the difference is the fields are not in same order
  outcome: it doesn't matter how the columns are ordered DataSet get craeted as long as the column type & count matches.
   */
  val reverseDs: Dataset[order_small] = tupleDS.as[order_small]
  reverseDs.show()


  /*
  Convert back Dataset to Dataframe without making huge change
   */

  val backToDataFrame = reverseDs.toDF()
  backToDataFrame.show()

  /*
  Rename DataFrame columns
   */

  val renameColmn = backToDataFrame.toDF("id", "status", "date")
  renameColmn.show()
}































