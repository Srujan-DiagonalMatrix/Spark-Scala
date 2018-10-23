package dataframe

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/*
  Functions for querying DateTime & TimeStamp columns.
*/


object DateTime {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DateTime").master("local[4]").getOrCreate()
    import spark.implicits._

    //Create a Schema type
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("dt", DateType, false),
        StructField("ts", TimestampType, false)
      )
    )

    //Create RDD of Rows
    val rows = spark.sparkContext.parallelize(
      Seq(
        Row(
          1,
          Date.valueOf("1999-01-11"),
          Timestamp.valueOf("2011-10-02 09:48:05.123456")
        ),

        Row(
          2,
          Date.valueOf("2004-04-14"),
          Timestamp.valueOf("2011-10-02 12:30:00.123456")
        ),

        Row(
          3,
          Date.valueOf("2008-12-31"),
          Timestamp.valueOf("2011-10-02 15:00:00.123456")
        )
      ), 4 //<<== num of partitions
    )

    //Create dataFrame
    val tdf = spark.createDataFrame(rows, schema)
    tdf.show()
    tdf.printSchema()

    //Pull DateType apart when querying
    tdf.select(
      $"dt",
      year($"dt").as("year"),
      quarter($"dt").as("Quarter"),
      month($"dt").as("Month"),
      weekofyear($"dt").as("Week of year"),
      dayofyear($"dt").as("Day of Year"),
      dayofmonth($"dt").as("Day of month")
    ).show()

    /*
    +----------+----+-------+-----+------------+-----------+------------+
    |        dt|year|Quarter|Month|Week of year|Day of Year|Day of month|
    +----------+----+-------+-----+------------+-----------+------------+
    |1999-01-11|1999|      1|    1|           2|         11|          11|
    |2004-04-14|2004|      2|    4|          16|        105|          14|
    |2008-12-31|2008|      4|   12|           1|        366|          31|
    +----------+----+-------+-----+------------+-----------+------------+
     */

    //Date Arthimetic operations
    tdf.select(
      datediff(current_date(), $"dt").as("Date diff"),
      date_sub($"dt", 2).as("Remove days"),
      date_add($"dt", 4).as("Add days"),
      add_months($"dt", 3).as("Add months")
    ).show()

    /*
    +---------+-----------+----------+----------+
    |Date diff|Remove days|  Add days|Add months|
    +---------+-----------+----------+----------+
    |     7221| 1999-01-09|1999-01-15|1999-04-11|
    |     5301| 2004-04-12|2004-04-18|2004-07-14|
    |     3579| 2008-12-29|2009-01-04|2009-03-31|
    +---------+-----------+----------+----------+
     */

    //Date truncation
    tdf.select(
      $"dt",
      trunc($"dt", "YYYY"),
      trunc($"dt", "YY"),
      trunc($"dt", "MM")
    ).show()

    /*
    +----------+---------------+-------------+-------------+
    |        dt|trunc(dt, YYYY)|trunc(dt, YY)|trunc(dt, MM)|
    +----------+---------------+-------------+-------------+
    |1999-01-11|     1999-01-01|   1999-01-01|   1999-01-01|
    |2004-04-14|     2004-01-01|   2004-01-01|   2004-04-01|
    |2008-12-31|     2008-01-01|   2008-01-01|   2008-12-01|
    +----------+---------------+-------------+-------------+
     */

    //Date formating
    tdf.select(
      $"dt".as("Date"),
      date_format($"dt", "YYYY/MM").as("YYYY/MM"),
      date_format($"dt", "yy/MM").as("yy/MM"),
      date_format($"dt", "MM/dd/YYYY").as("MM/dd/YYYY")
    ).show()
    /*
    +----------+-------+-----+----------+
    |      Date|YYYY/MM|yy/MM|MM/dd/YYYY|
    +----------+-------+-----+----------+
    |1999-01-11|1999/01|99/01|01/11/1999|
    |2004-04-14|2004/04|04/04|04/14/2004|
    |2008-12-31|2009/12|08/12|12/31/2009|
    +----------+-------+-----+----------+
     */


    //Pull timstamp apart when querying
    tdf.select(
      $"dt",
      year($"ts").as("Year"),
      hour($"ts").as("Hour"),
      minute($"ts").as("Month"),
      second($"ts").as("Second")
    ).show()
    /*
    +----------+----+----+-----+------+
    |        dt|Year|Hour|Month|Second|
    +----------+----+----+-----+------+
    |1999-01-11|2011|   9|   48|     5|
    |2004-04-14|2011|  12|   30|     0|
    |2008-12-31|2011|  15|    0|     0|
    +----------+----+----+-----+------+
     */


  }

}
