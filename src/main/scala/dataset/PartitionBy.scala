package dataset

import org.apache.spark.sql.SparkSession
import java.io.File

object PartitionBy {

  case class Transaction(id: Long, year: Int, month: Int, day: Int, qty: Long, price: Double)

  def main(args: Array[String]): Unit = {

    //Output stores here
    val exampleRoot = "/tmp/LearningSpark"

    val spark = SparkSession.builder().appName("DataSet - PartitionBy").master("local[4]").config("spark.default.parallelism",4).getOrCreate()
    import spark.implicits._

    utils.PartitionedTableHierarchy.delteRecursively(new File(exampleRoot))

    val transactions = Seq(
      // 2016-11-05
      Transaction(1001, 2016, 11, 5, 100, 42.99),
      Transaction(1002, 2016, 11, 5, 75, 42.99),
      // 2016-11-15
      Transaction(1003, 2016, 11, 15, 50, 75.95),
      Transaction(1004, 2016, 11, 15, 50, 19.95),
      Transaction(1005, 2016, 11, 15, 25, 42.99),
      // 2016-12-11
      Transaction(1006, 2016, 12, 11, 22, 11.00),
      Transaction(1007, 2016, 12, 11, 100, 170.00),
      Transaction(1008, 2016, 12, 11, 50, 5.99),
      Transaction(1009, 2016, 12, 11, 10, 11.00),
      // 2016-12-22
      Transaction(1010, 2016, 12, 22, 20, 10.99),
      Transaction(1011, 2016, 12, 22, 10, 75.95),
      // 2017-01-01
      Transaction(1012, 2017, 1, 2, 1020, 9.99),
      Transaction(1013, 2017, 1, 2, 100, 19.99),
      // 2017-01-31
      Transaction(1014, 2017, 1, 31, 200, 99.95),
      Transaction(1015, 2017, 1, 31, 80, 75.95),
      Transaction(1016, 2017, 1, 31, 200, 100.95),
      // 2017-02-01
      Transaction(1017, 2017, 2, 1, 15, 22.00),
      Transaction(1018, 2017, 2, 1, 100, 75.95),
      Transaction(1019, 2017, 2, 1, 5, 22.00),
      // 2017-02-22
      Transaction(1020, 2017, 2, 22, 5, 42.99),
      Transaction(1021, 2017, 2, 22, 100, 42.99),
      Transaction(1022, 2017, 2, 22, 75, 11.99),
      Transaction(1023, 2017, 2, 22, 50, 42.99),
      Transaction(1024, 2017, 2, 22, 200, 99.95)
    )

    val transactionDS = spark.createDataset(transactions)

    //Num of partitions come from default parallalism
    println(transactionDS.rdd.getNumPartitions)
    println(transactionDS.rdd.partitions.size)

    val simpleRoot = exampleRoot+"/Simple"
    transactionDS.write.option("header","true").csv("simpleRoot")
    //file count
    utils.PartitionedTableHierarchy.countRecursively(new File(simpleRoot), ".csv")
    //Print files
    utils.PartitionedTableHierarchy.printRecursively(new File(simpleRoot))



    val partitionedRoot = exampleRoot+"/Partitioned"
    transactionDS.write.option("header","true").csv("partitionedRoot")
    utils.PartitionedTableHierarchy.countRecursively(new File(partitionedRoot), ".csv")
    utils.PartitionedTableHierarchy.printRecursively(new File(partitionedRoot))


    val repartitionedRoot = exampleRoot+"/Repartitioned"
    transactionDS.repartition($"year", $"month").write.partitionBy("year","month").option("header","true").csv("repartitionedRoot")
    utils.PartitionedTableHierarchy.printRecursively(new File(repartitionedRoot))


    val allDF = spark.read.option("basePath","partitionedRoot").option("header","true").csv(partitionedRoot)
    allDF.show()


    val oneDF = spark.read.option("basePath",partitionedRoot+"/year=2016/month=11").option("header","true").csv(partitionedRoot+"/year=2016/month=11")
    oneDF.show()

    val oneMonth = spark.read.option("basePath",partitionedRoot).option("header","true").csv(partitionedRoot + "year=2016/month=11")
    oneMonth.show()

    val twoMonthQuarter = spark.read.option("basePath",partitionedRoot).option("header","true").csv(partitionedRoot+"year=2016/month=11")
    twoMonthQuarter.show()

  }
}
