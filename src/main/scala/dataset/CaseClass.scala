package dataset

import org.apache.spark.sql.SparkSession

object CaseClass {

  case class Number(i: Int, english: String, french: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Dataset-Case class").master("local[4]").getOrCreate()
    import spark.implicits._

    val numbers = Seq(
      Number(1, "one", "un"),
      Number(2, "two", "deux"),
      Number(3, "three", "trois")
    )

    /*
    2 ways to create DataSets
    */

    //1st way
      val num = numbers.toDS()

    //2nd way
    spark.createDataset(numbers)

    //DataTypes
    num.dtypes.foreach(println(_))

    //Write condistion and select fields
    num.where($"i" > 2).select($"english", $"french")


  }
}
