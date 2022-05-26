
import Generator.getClass
import vegas._
import vegas.sparkExt._
import vegas.DSL.UnitSpecBuilder

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

import java.io.File
import scala.collection.mutable.ArrayBuffer


object DataVisualizer {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark-Vegas Data Visualizer")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()

  /*val df = spark.read.csv("data.csv")
  df.createOrReplaceTempView("Orders")
  df.registerTempTable("Orders")
  val dfQ2_1 = spark.sql ("SELECT _c9,_c4, sum(_c7) FROM Orders where _c9 like '2021%' AND _c4 = 'Cutting board'  group by _c9").toDF("Product","Time","Products Sold")
  dfQ2_1.show()
  val dfQ1_1 = spark.sql("SELECT _c5, sum(_c7) FROM Orders group by _c5").toDF("Category","Products Sold")
  //dfQ1_1.show()
  val dfQ1_2 = spark.sql("SELECT _c11, _c5, sum(_c7) FROM Orders group by _c11, _c5").toDF("Country","Category","Products Sold")
  //dfQ1_2.show()*/

  // Sample Data
  val sample_data1: Seq[(String, Int)] = Seq(("stationery", 10), ("household products", 3), ("groceries", 5))
  val sample_data2: Seq[(String, Int)] = Seq(("00:00",50), ("01:00", 120), ("02:00",150), ("03:00",400))
  val sample_data3: Seq[(String, Int)] = Seq(("00:00",20), ("01:00", 100), ("02:00",350), ("03:00",470))
  val sample_data4: Seq[(String, Int)] = Seq(("00:00",30), ("01:00", 110), ("02:00",250), ("03:00",450))
  val sample_df1: DataFrame = spark.createDataFrame(sample_data1).toDF("Category Name", "Products Sold")
  val sample_df2: DataFrame = spark.createDataFrame(sample_data2).toDF("Time", "Access")
  val sample_df3: DataFrame = spark.createDataFrame(sample_data3).toDF("Time", "Access")


  //val df = spark.read.csv("data.csv")
  val df: DataFrame = spark.read.format("csv")
    .option("header","false")
    .load("C:\\Users\\Erienne Work\\Documents\\Revature\\Training Projects\\Project2\\data.csv")
    .toDF("order_id","customer_id","customer_name",
      "product_id","product_name", "product_category","payment_type","qty","product_price",
      "datetime","country","city",
      "website","txn_id","txn_success","failure_reason")

  df.createOrReplaceTempView("orders")

  /*df.createOrReplaceTempView("Orders")
  df.registerTempTable("Orders")
  val sqlDF = spark.sql ("SELECT * FROM Orders where _c6 = 'Wallet'")
  sqlDF.show()*/

  def main(args: Array[String]): Unit = {
    //plotBarChart(sample_df1)
    //plotLineChart(sample_df2)
    //plotMultiLineChart(Array(sample_df2, sample_df3, sample_df4))
    queryTopSellingProductByCountry()
  }
  def queryTopSellingProduct(): Unit = {
    val query_selection_df: DataFrame = spark.sql("SELECT product_category, COUNT(order_id) AS quantity FROM orders WHERE product_category != 'null' AND country != 'null' GROUP BY product_category")
    plotBarChart(query_selection_df)
  }

  def queryTopSellingProductByCountry(): Unit = {
    val query_selection_df: DataFrame = spark.sql("SELECT country, COUNT(order_id) AS quantity, product_category FROM orders WHERE product_category != 'null' AND country != 'null' GROUP BY country, product_category")
    plotBarChart(query_selection_df, has_categories = true, category_filter = "product_category")
  }

  def getPlotInfo(df: DataFrame): ArrayBuffer[spec.Spec.Type] = {
    val col_name_array = df.columns
    val data_type_array: ArrayBuffer[spec.Spec.Type] = ArrayBuffer()

    for (column <- col_name_array) {
      val my_data_type = df.schema(column).dataType
      my_data_type match {
        case StringType => data_type_array += Nom
        case IntegerType => data_type_array += Quant
        case DoubleType => data_type_array += Quant
        case LongType => data_type_array += Quant
        case FloatType => data_type_array += Quant
        case _ => println("Not a recognized type")
      }
    }

    data_type_array
  }

  def plotBarChart(df: DataFrame, has_categories: Boolean = false, category_filter: String = "", chart_title: String = ""): Unit = {
    val col_name_array = df.columns
    val data_type_array = getPlotInfo(df)

    if (has_categories) {
      val plot = Vegas(chart_title)
        .withDataFrame(df)
        .encodeX(col_name_array(0), data_type_array(0))
        .encodeY(col_name_array(1), data_type_array(1))
        .mark(Bar)
        .encodeColor(field=category_filter, dataType=Nominal)

      plot.show

    } else {
      val plot = Vegas(chart_title)
        .withDataFrame(df)
        .encodeX(col_name_array(0), data_type_array(0))
        .encodeY(col_name_array(1), data_type_array(1))
        .mark(Bar)

      plot.show
    }
  }

  def plotLineChart(df: DataFrame, chart_title: String = ""): Unit = {
    val col_name_array = df.columns
    val data_type_array = getPlotInfo(df)

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
    val data_type_array = getPlotInfo(df)

    val my_layer = Layer()
      .withDataFrame(df)
      .encodeX(col_name_array(0), data_type_array(0))
      .encodeY(col_name_array(1), data_type_array(1))
      .mark(Line)

    my_layer
  }
}