package dataset

/*
Creating DataSet from a complex nested class.
*/

object ComplexTypes {

  case class Point(x: Double, y: Double)
  case class Segment(from: Point, to: Point)
  case class Line(name: String, points: Array[Point])
  case class NamedPoints(name: String, points: Map[String, Point])
  case class NameAndMayBePoint(name: String, point: Option[Point])

  def main(args: Array[String]): Unit = {




  }
}
