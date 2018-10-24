package dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
Creating DataSet from a complex nested classes, arrays, maps, option and query them.
*/

object ComplexTypes {

  case class Point(x: Double, y: Double)
  case class Segment(from: Point, to: Point)
  case class Line(name: String, points: Array[Point])
  case class NamedPoints(name: String, points: Map[String, Point])
  case class NameAndMayBePoint(name: String, point: Option[Point])

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Dataset - ComplexTypes").master("local[4]").getOrCreate()
    import spark.implicits._

  /*
    ========================================================================
    Example 1: Nested case classes
    ========================================================================
    Segment is a case class which uses Point Case class as an arguments.
   */

    val segments = Seq(
      Segment(Point(1.0, 2.0), Point(3.0, 4.0)),
      Segment(Point(8.0, 2.0), Point(3.0, 14.0)),
      Segment(Point(11.0, 2.0), Point(3.0, 24.0))
    )
    val segmentDS = segments.toDS()
    segmentDS.printSchema()

    /*
    root
     |-- from: struct (nullable = true)
     |    |-- x: double (nullable = false)
     |    |-- y: double (nullable = false)
     |-- to: struct (nullable = true)
     |    |-- x: double (nullable = false)
     |    |-- y: double (nullable = false)
    */

    //Apply a where clause on Segmen class's from field, which is nothing but Point class.
    segmentDS.where($"from".getField("x") > 7.0).select($"to").show()
    /*
    +-----------+
    |         to|
    +-----------+
    |[3.0, 14.0]|
    |[3.0, 24.0]|
    +-----------+
    */

    /*
    ========================================================================
    Example 2: Arrays
    ========================================================================
    */

    val lines = Seq(
      Line("a", Array(Point(1.0, 2.0), Point(3.0, 4.0))),
      Line("b", Array(Point(8.0, 2.0), Point(3.0, 14.0))),
      Line("c", Array(Point(11.0, 2.0), Point(3.0, 24.0), Point(10.0, 100.0)))
    )

    val linesDS = spark.createDataset(lines)
    linesDS.printSchema()

    linesDS.where($"points".getItem(2).getField("y") > 7.0).select($"name", size($"points").as("count")).show()
    /*
    +----+-----+
    |name|count|
    +----+-----+
    |   c|    3|
    +----+-----+
    */


    /*
    ========================================================================
    Example 3: Maps
    ========================================================================
    */

    val namedPoints = Seq(
      NamedPoints("a", Map("p1" -> Point(0.0, 0.0))),
      NamedPoints("b", Map("p1" -> Point(0.0, 0.0), "p2" -> Point(2.0, 6.0), "p3" -> Point(10.0, 100.0)))
    )
    val namedPointsDS = namedPoints.toDS()
    namedPointsDS.printSchema()

    namedPointsDS.where(size($"points") > 1).select(size($"points").as("count"), $"points".getItem("p1")).show()
    /*
    +-----+----------+
    |count|points[p1]|
    +-----+----------+
    |    3|[0.0, 0.0]|
    +-----+----------+
    */


    /*
    ========================================================================
    Example 4: Option
    case class NameAndMayBePoint(name: String, point: Option[Point])
    case class Point(x: Double, y: Double)
    ========================================================================
    */

    val maybePoints = Seq(
      NameAndMayBePoint("p1", None),
      NameAndMayBePoint("p2", Some(Point(-3.1, 99.99))),
      NameAndMayBePoint("p3", Some(Point(1.0, 2.0))),
      NameAndMayBePoint("p4", None)
    )

    val mayBePointsDS = maybePoints.toDS()
    mayBePointsDS.printSchema()

    mayBePointsDS.where($"point".getField("y") > 50).select($"name", $"point").show()
    /*
    +----+-------------+
    |name|        point|
    +----+-------------+
    |  p2|[-3.1, 99.99]|
    +----+-------------+
     */

    mayBePointsDS.select($"point".getField("x")).show()
    /*
    +-------+
    |point.x|
    +-------+
    |   null|
    |   -3.1|
    |    1.0|
    |   null|
    +-------+
    */
  }
}
