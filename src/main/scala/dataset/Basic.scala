package dataset

import org.apache.spark.sql.SparkSession

object Basic {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataSet - Basic").master("local[4]").getOrCreate()
    import spark.implicits._

    //create a tiny dataset of integers
    val s = Seq(10,11,12,13,14,15).toDS()
    s.show()
    s.printSchema()

    /*
    +-----+
    |value|
    +-----+
    |   10|
    |   11|
    |   12|
    |   13|
    |   14|
    |   15|
    +-----+
    */

    //View columns and dataTypes
    s.columns.foreach(println(_)) //value <- This is the default name of each column
    s.dtypes.foreach(println(_)) //(value,IntegerType)

    //Filter data
    s.where($"value" > 12).show()

    //Convert tuple to DataSet
    val tuples = Seq((1,"One","un"), (2, "two", "deux"), (3, "three", "trois")).toDS()
    tuples.dtypes.foreach(println(_))
    /*
    (_1,IntegerType)
    (_2,StringType)
    (_3,StringType)
     */

    //Query tuple
    tuples.where($"_1" > 1).select($"_2", $"_3").show()
    /*
    +-----+-----+
    |   _2|   _3|
    +-----+-----+
    |  two| deux|
    |three|trois|
    +-----+-----+
     */






  }
}
