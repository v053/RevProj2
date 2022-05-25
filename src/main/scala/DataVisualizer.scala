import vegas._
import vegas.sparkExt._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataVisualizer {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark-Vegas Data Visualizer")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()

  val sample_data1: Seq[(String, Int)] = Seq(("stationery", 10), ("household products", 3), ("groceries", 5))
  val sample_df1: DataFrame = spark.createDataFrame(sample_data1).toDF("Category Name", "Products Sold")

  def main(args: Array[String]): Unit = {
    val plot = Vegas("Chart Name")
      .withDataFrame(sample_df1: DataFrame)
      .encodeX("Category Name", Nom)
      .encodeY("Products Sold", Quantitative)
      .mark(Bar)

    //plot.show
    plotBarChart(sample_df1)
  }

  def plotBarChart(df: DataFrame): Unit = {
    val plot = Vegas("")
      .withDataFrame(df)
      .encodeX("Category Name", Nom)
      .encodeY("Products Sold", Quantitative)
      .mark(Bar)

    plot.show
  }
}
