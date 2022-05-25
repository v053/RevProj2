import vegas._
import vegas.sparkExt._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import vegas.DSL.UnitSpecBuilder

import scala.collection.mutable.ArrayBuffer


object DataVisualizer {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark-Vegas Data Visualizer")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()

  val sample_data1: Seq[(String, Int)] = Seq(("stationery", 10), ("household products", 3), ("groceries", 5))
  val sample_data2: Seq[(String, Int)] = Seq(("00:00",50), ("01:00", 120), ("02:00",150), ("03:00",400))
  val sample_data3: Seq[(String, Int)] = Seq(("00:00",20), ("01:00", 100), ("02:00",350), ("03:00",470))
  val sample_data4: Seq[(String, Int)] = Seq(("00:00",30), ("01:00", 110), ("02:00",250), ("03:00",450))
  val sample_df1: DataFrame = spark.createDataFrame(sample_data1).toDF("Category Name", "Products Sold")
  val sample_df2: DataFrame = spark.createDataFrame(sample_data2).toDF("Time", "Access")
  val sample_df3: DataFrame = spark.createDataFrame(sample_data3).toDF("Time", "Access")
  val sample_df4: DataFrame = spark.createDataFrame(sample_data4).toDF("Time", "Access")

  def main(args: Array[String]): Unit = {
    //plotBarChart(sample_df1)
    //plotLineChart(sample_df2)
    plotMultiLineChart(Array(sample_df2, sample_df3, sample_df4))
  }

  def plotBarChart(df: DataFrame, chart_title: String = ""): Unit = {
    val col_name_array = df.columns
    val data_type_array: ArrayBuffer[spec.Spec.Type] = ArrayBuffer()

    for (column <- col_name_array) {
      val my_data_type = df.schema(column).dataType
      my_data_type match {
        case StringType => data_type_array += Nom
        case IntegerType => data_type_array += Quant
        case _ => println("Not a recognized type")
      }
    }

    val plot = Vegas(chart_title)
      .withDataFrame(df)
      .encodeX(col_name_array(0), data_type_array(0))
      .encodeY(col_name_array(1), data_type_array(1))
      .mark(Bar)

    plot.show
  }

  def plotLineChart(df: DataFrame, chart_title: String = ""): Unit = {
    val col_name_array = df.columns
    val data_type_array: ArrayBuffer[spec.Spec.Type] = ArrayBuffer()

    for (column <- col_name_array) {
      val my_data_type = df.schema(column).dataType
      my_data_type match {
        case StringType => data_type_array += Nom
        case IntegerType => data_type_array += Quant
        case _ => println("Not a recognized type")
      }
    }

    val plot = Vegas(chart_title)
      .withDataFrame(df)
      .encodeX(col_name_array(0), data_type_array(0))
      .encodeY(col_name_array(1), data_type_array(1))
      .mark(Line)

    plot.show
  }

  def plotMultiLineChart(df_array: Array[DataFrame], chart_title: String = ""): Unit = {
    val colors = Array("0653BE","BE06AF", "#BE7106", "06BE15", "36CCF0")
    val all_layers: ArrayBuffer[UnitSpecBuilder] = ArrayBuffer()
    for(df <- df_array) {
      val layer = createGraphLayer(df)
      all_layers += layer.encodeColor(value = colors(df_array.indexOf(df)%colors.length))
    }

    val layers_array: Array[UnitSpecBuilder] = all_layers.toArray

    val plot = Vegas.layered(chart_title)
      .withLayers(
        layers_array: _*
      )

    plot.show
  }

  def createGraphLayer(df: DataFrame): UnitSpecBuilder = {
    val col_name_array = df.columns
    val data_type_array: ArrayBuffer[spec.Spec.Type] = ArrayBuffer()

    for (column <- col_name_array) {
      val my_data_type = df.schema(column).dataType
      my_data_type match {
        case StringType => data_type_array += Nom
        case IntegerType => data_type_array += Quant
        case _ => println("Not a recognized type")
      }
    }

    val my_layer = Layer()
      .withDataFrame(df)
      .encodeX(col_name_array(0), data_type_array(0))
      .encodeY(col_name_array(1), data_type_array(1))
      .mark(Line)

    my_layer
  }
}
