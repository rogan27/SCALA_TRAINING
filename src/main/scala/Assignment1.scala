package org.example.scala

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}
import java.sql.Date


object Assignment1 {
  val dw_dir = "file:///G:\\Ashok\\TRAININGS\\JIGSAW\\ASSIGNMENTS\\1st Assignment\\INPUTS\\dataset"
  val superstore_Returns_path = dw_dir + "\\Global Superstore Sales - Global Superstore Returns.csv"
  val superstore_Sales_path = dw_dir + "\\Global Superstore Sales - Global Superstore Sales.csv"
def main(args: Array[String]) = {
  val winutilPath = "G:\\Ashok\\TRAININGS\\JIGSAW\\PACKAGES\\winutils"

  if (System.getProperty("os.name").toLowerCase.contains("win")) {
    System.out.println("Detected windows")
    System.setProperty("hadoop.home.dir", winutilPath)
    System.setProperty("HADOOP_HOME", winutilPath)
  }

  val spark = SparkSession.builder
    .appName("Simple Application")
    .master("local[*]")
    .getOrCreate()


  windows_agg(spark)
}

  def windows_agg(spark: SparkSession) = {
    val superstore_ReturnsDf = spark.read.option("header", "true").option("inferSchema", "true").csv(superstore_Returns_path)
    val superstore_SalesDf = spark.read.option("header", "true").option("inferSchema", "true").csv(superstore_Sales_path)
    //superstore_ReturnsDf.show()
    //superstore_SalesDf.show()

    val joinedDf = superstore_ReturnsDf.join(superstore_SalesDf, "Order ID")

    joinedDf.show()
    //val cubedDf = joinedDf.cube("Profit", "Quantity").sum()
    //cubedDf.orderBy("Country").show()

  /*
    val rankSpec: WindowSpec = Window.partitionBy("date_of_sale").
      orderBy(functions.col("total_amount").desc)
    val rankSpecDf: Dataset[Row] = superstore_ReturnsDf
      .withColumn("date_wise_rank", functions.dense_rank.over(rankSpec))
      .where("date_wise_rank = 1")
    rankSpecDf.show()

   */
  }
}
