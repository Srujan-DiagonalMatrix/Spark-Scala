package dataframe

import org.apache.spark.sql.SparkSession

object Range {
  def main(args: Array[String]): Unit = {

    /*
    A method on SQLContext creating a dataframe with a single column
    */

    val spark = SparkSession.builder().appName("DataFrame - range").master("local[4]").getOrCreate()
    import spark.implicits._

    //A default cloumn range creates is 'id'
    val df1 = spark.range(10,14)
    df1.show()
    println(df1.rdd.partitions.length) //4
    /*
    +---+
    | id|
    +---+
    | 10|
    | 11|
    | 12|
    | 13|
    +---+
    */

    //Stepped range
    val df2 = spark.range(10,20,2)
    df2.show()
    /*
    +---+
    | id|
    +---+
    | 10|
    | 12|
    | 14|
    | 16|
    | 18|
    +---+
     */

    //Stepped range with num of partitions
    val df3 = spark.range(10,20,2,4)
    df3.show()
    println(df3.rdd.partitions.length) //4

    /*
    +---+
    | id|
    +---+
    | 10|
    | 12|
    | 14|
    | 16|
    | 18|
    +---+
     */

    //Single argument is end of range
    val df4 = spark.range(10)
    df4.show()
  /*
    +---+
    | id|
    +---+
    |  0|
    |  1|
    |  2|
    |  3|
    |  4|
    |  5|
    |  6|
    |  7|
    |  8|
    |  9|
    +---+
   */

  }
}
