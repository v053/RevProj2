
import vegas._
import vegas.sparkExt._
import vegas.DSL.{ExtendedUnitSpecBuilder, UnitSpecBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

import java.io.{File, PrintWriter}
import scala.collection.mutable.ArrayBuffer


object DataVisualizer {

  //val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark-Vegas Data Visualizer")
    .config("spark.master", "local")
    //.config("spark.sql.dir.warehouse")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._
  import spark.sql

  var all_plots: ArrayBuffer[ExtendedUnitSpecBuilder] = ArrayBuffer()
  var all_layered_plots: ArrayBuffer[vegas.DSL.LayerSpecBuilder] = ArrayBuffer()

  /*val df = spark.read.csv("data.csv")
  df.createOrReplaceTempView("Orders")
  df.registerTempTable("Orders")*/


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
    .option("inferSchema", "true")
    .load("C:\\Users\\Erienne Work\\Documents\\Revature\\Training Projects\\Project2\\data.csv")
    //.load("data.csv")
    .toDF("order_id","customer_id","customer_name",
      "product_id","product_name", "product_category","payment_type","qty","product_price",
      "datetime","country","city",
      "website","txn_id","txn_success","failure_reason")

  df.createOrReplaceTempView("orders")
  //sql("CREATE DATABASE IF NOT EXISTS project_2_db")
  //sql("USE project_2_db")
  //df.write.mode("overwrite").saveAsTable("orders_hive")


  // val dfQ2_1_21_J = spark.sql (
  //   "SELECT datetime, qty, product_name FROM Orders where datetime like '2021-01%'").toDF("Time","Products Sold","Product Name")
  // dfQ2_1_21_J.show()
  val dfQ1_1 = spark.sql("SELECT product_category, sum(qty) FROM orders group by product_category").toDF("Category","Products Sold")
  //dfQ1_1.show()
  val dfQ1_2 = spark.sql("SELECT country, product_category, sum(qty) FROM Orders group by country, product_category").toDF("Country","Category","Products Sold")
  //dfQ1_2.show()

  def main(args: Array[String]): Unit = {
    queryTopSellingProduct()
    queryTopSellingProductByCountry()
    Question2()
    queryHighestTrafficOfSales()
    //sql("SELECT * FROM orders_hive").show()

    //showAndWriteAllToHTML()
  }

  // Queries
  def queryTopSellingProduct(): Unit = {
    val query_selection_df: DataFrame = spark.sql("SELECT product_category, SUM(qty) AS quantity FROM orders WHERE product_category != 'null' AND country != 'null' GROUP BY product_category")
    plotBarChart(query_selection_df, chart_title = "Top Selling Product Categories")
  }

  def queryTopSellingProductByCountry(): Unit = {
    val query_selection_df: DataFrame = spark.sql("SELECT country, SUM(qty) AS quantity, product_category FROM orders WHERE product_category != 'null' AND country != 'null' GROUP BY country, product_category")
    plotBarChart(query_selection_df,has_categories = true,"product_category","Top Product Category by Country")
  }

   def queryHighestTrafficOfSales():  Unit = {
      val query_selection_df: DataFrame = spark.sql("SELECT city, SUM(qty) AS quantity, product_category FROM orders WHERE product_category != 'null' AND city != 'null' GROUP BY city, product_category")
      plotBarChart(query_selection_df,has_categories = true,"Products Sold","Highest traffic of Sales")
    }


  // Plotting
  def getPlotInfo(df: DataFrame): ArrayBuffer[spec.Spec.Type] = {
    val col_name_array = df.columns
    val data_type_array: ArrayBuffer[spec.Spec.Type] = ArrayBuffer()

    for (column <- col_name_array) {
      val my_data_type = df.schema(column).dataType
      //println(column + ", " + my_data_type)
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
        .encodeColumn(col_name_array(0), Nominal, scale=Scale(padding=4.0), axis=Axis(orient=Orient.Bottom, axisWidth=1.0, offset= -8.0))
        .encodeY(col_name_array(1), data_type_array(1))
        .encodeX(category_filter, Nominal, hideAxis=true)
        .encodeColor(category_filter, Nominal)
        .mark(Bar)

      //val raw_html = plot.html.plotHTML(chart_title)
      all_plots += plot

      plot.show

    } else {
      val plot = Vegas(chart_title)
        .withDataFrame(df)
        .encodeX(col_name_array(0), data_type_array(0))
        .encodeY(col_name_array(1), data_type_array(1))
        .encodeColor(col_name_array(0), data_type_array(0))
        .mark(Bar)

      //val raw_html = plot.html.plotHTML(chart_title)
      all_plots += plot

      plot.show
    }
  }

  def plotLineChart(df: DataFrame, has_categories: Boolean = false, category_filter: String = "", chart_title: String = ""): Unit = {
    val col_name_array = df.columns
    val data_type_array = getPlotInfo(df)

    if (has_categories) {
      val plot = Vegas(chart_title)
        .withDataFrame(df)
        .encodeX(col_name_array(0), data_type_array(0))
        .encodeY(col_name_array(1), data_type_array(1))
        .mark(Line)
        .encodeDetailFields(Field(field=category_filter, dataType=Nominal))
        .encodeColor(category_filter, Nominal)

      //val raw_html = plot.html.plotHTML(chart_title)
      all_plots += plot
      plot.show

    } else {
      val plot = Vegas(chart_title)
        .withDataFrame(df)
        .encodeX(col_name_array(0), data_type_array(0))
        .encodeY(col_name_array(1), data_type_array(1))
        .mark(Line)

      //val raw_html = plot.html.plotHTML(chart_title)
      all_plots += plot

      plot.show
    }
  }

  def plotMultiLineChart(df_array: Array[DataFrame], chart_title: String = ""):Unit  = {
    val colors = Array("0653BE","BE06AF", "#BE7106", "06BE15", "36CCF0")
    val all_layers: ArrayBuffer[UnitSpecBuilder] = ArrayBuffer()
    for(df <- df_array) {
      val layer = createGraphLayer(df, Line)
      all_layers += layer.encodeColor(value = colors(df_array.indexOf(df)%colors.length))
    }

    val layers_array: Array[UnitSpecBuilder] = all_layers.toArray

    val plot = Vegas.layered(chart_title)
      .withLayers(
        layers_array: _*
      )

    plot.show
    all_layered_plots += plot
  }

  def createGraphLayer(df: DataFrame, mark_type: spec.Spec.Mark): UnitSpecBuilder = {
    val col_name_array = df.columns
    val data_type_array = getPlotInfo(df)

    val my_layer = Layer()
      .withDataFrame(df)
      .encodeX(col_name_array(0), data_type_array(0))
      .encodeY(col_name_array(1), data_type_array(1))
      .mark(mark_type)

    my_layer
  }

  def Question2(): Unit = {
    //Views for 2021 Averages
    var CB: DataFrame = spark.createDataFrame(Months2021("Cutting board")).toDF("Month", "Product_Sold")
    val Mop: DataFrame = spark.createDataFrame(Months2021("Mop")).toDF("Month", "Access")
    val WC: DataFrame = spark.createDataFrame(Months2021("Window cleaner")).toDF("Month", "Access")
    val Computer_case: DataFrame = spark.createDataFrame(Months2021("Computer case")).toDF("Month", "Access")
    val Dinner_plates: DataFrame = spark.createDataFrame(Months2021("Dinner plates")).toDF("Month", "Access")
    val Keyboard: DataFrame = spark.createDataFrame(Months2021("Keyboard")).toDF("Time", "Access")
    val Screws: DataFrame = spark.createDataFrame(Months2021("Screws")).toDF("Time", "Access")
    val Speaker: DataFrame = spark.createDataFrame(Months2021("Speaker")).toDF("Time", "Access")
    val Ladder: DataFrame = spark.createDataFrame(Months2021("Ladder")).toDF("Time", "Access")
    val Laptop: DataFrame = spark.createDataFrame(Months2021("Laptop")).toDF("Time", "Access")
    val Monitor: DataFrame = spark.createDataFrame(Months2021("Monitor")).toDF("Time", "Access")
    val Mixing_bowl: DataFrame = spark.createDataFrame(Months2021("Mixing bowl")).toDF("Time", "Access")
    val Charger: DataFrame = spark.createDataFrame(Months2021("Charger")).toDF("Time", "Access")
    val Measuring_cup: DataFrame = spark.createDataFrame(Months2021("Measuring cup")).toDF("Time", "Access")
    val Mouse: DataFrame = spark.createDataFrame(Months2021("Mouse")).toDF("Time", "Access")
    val Box_cutter: DataFrame = spark.createDataFrame(Months2021("Box cutter")).toDF("Time", "Access")
    val Ice_cube_trays: DataFrame = spark.createDataFrame(Months2021("Ice cube trays")).toDF("Time", "Access")
    val Printer: DataFrame = spark.createDataFrame(Months2021("Printer")).toDF("Time", "Access")
    plotMultiLineChart(Array(CB,Mop,WC,Computer_case,Dinner_plates))

    //Views for Overall Averages
    var CBA: DataFrame = spark.createDataFrame(Years("Cutting board")).toDF("Year", "Product_Sold")
    val MopA: DataFrame = spark.createDataFrame(Years("Mop")).toDF("Year", "Access")
    val WCA: DataFrame = spark.createDataFrame(Years("Window cleaner")).toDF("Year", "Access")
    val Computer_caseA: DataFrame = spark.createDataFrame(Years("Computer case")).toDF("Year", "Access")
    val Dinner_platesA: DataFrame = spark.createDataFrame(Years("Dinner plates")).toDF("Year", "Access")
    plotMultiLineChart(Array(CBA,MopA,WCA,Computer_caseA,Dinner_platesA))

  }

  def Drop(): Unit = {
    var temp = spark.sql("DROP VIEW IF EXISTS 2021J")
    temp = spark.sql("DROP VIEW IF EXISTS 2021F")
    temp = spark.sql("DROP VIEW IF EXISTS 2021M")
    temp = spark.sql("DROP VIEW IF EXISTS 2021A")
    temp = spark.sql("DROP VIEW IF EXISTS 2021Ma")
    temp = spark.sql("DROP VIEW IF EXISTS 2021Ju")
    temp = spark.sql("DROP VIEW IF EXISTS 2021Jy")
    temp = spark.sql("DROP VIEW IF EXISTS 2021Au")
    temp = spark.sql("DROP VIEW IF EXISTS 2021Se")
    temp = spark.sql("DROP VIEW IF EXISTS 2021O")
    temp = spark.sql("DROP VIEW IF EXISTS 2021N")
    temp = spark.sql("DROP VIEW IF EXISTS 2021De")
  }

  def Months2021(Product: String): Seq[(String, Double)] = {
    val dfQ2_1_21_J = spark.sql (
      "CREATE TEMPORARY VIEW 2021J AS SELECT datetime, qty FROM Orders where datetime like '2021-01%' AND product_name = '"+Product+"'")
    val  J2021 = Averages("2021J")
    val dfQ2_1_21_F = spark.sql (
      "CREATE TEMPORARY VIEW 2021F AS SELECT datetime, qty FROM Orders where datetime like '2021-02%' AND product_name = '"+Product+"'")
    val  F2021 = Averages("2021F")
    val dfQ2_1_21_M = spark.sql (
      "CREATE TEMPORARY VIEW 2021M AS SELECT datetime, qty FROM Orders where datetime like '2021-03%' AND product_name = '"+Product+"'")
    val  M2021 = Averages("2021M")
    val dfQ2_1_21_A = spark.sql (
      "CREATE TEMPORARY VIEW 2021A AS SELECT datetime, qty FROM Orders where datetime like '2021-04%' AND product_name = '"+Product+"'")
    val  A2021 = Averages("2021A")
    val dfQ2_1_21_Ma = spark.sql (
      "CREATE TEMPORARY VIEW 2021Ma AS SELECT datetime, qty FROM Orders where datetime like '2021-05%' AND product_name = '"+Product+"'")
    val  Ma2021 = Averages("2021Ma")
    val dfQ2_1_21_Ju = spark.sql (
      "CREATE TEMPORARY VIEW 2021Ju AS SELECT datetime, qty FROM Orders where datetime like '2021-06%' AND product_name = '"+Product+"'")
    val  Ju2021 = Averages("2021Ju")
    val dfQ2_1_21_Jy = spark.sql (
      "CREATE TEMPORARY VIEW 2021Jy AS SELECT datetime, qty FROM Orders where datetime like '2021-07%' AND product_name = '"+Product+"'")
    val  Jy2021 = Averages("2021Jy")
    val dfQ2_1_21_Au = spark.sql (
      "CREATE TEMPORARY VIEW 2021Au AS SELECT datetime, qty FROM Orders where datetime like '2021-08%' AND product_name = '"+Product+"'")
    val  Au2021 = Averages("2021Au")
    val dfQ2_1_21_S = spark.sql (
      "CREATE TEMPORARY VIEW 2021Se AS SELECT datetime, qty FROM Orders where datetime like '2021-09%' AND product_name = '"+Product+"'")
    val  S2021 = Averages("2021Se")
    val dfQ2_1_21_O = spark.sql (
      "CREATE TEMPORARY VIEW 2021O AS SELECT datetime, qty FROM Orders where datetime like '2021-10%' AND product_name = '"+Product+"'")
    val  O2021 = Averages("2021O")
    val dfQ2_1_21_N = spark.sql (
      "CREATE TEMPORARY VIEW 2021N AS SELECT datetime, qty FROM Orders where datetime like '2021-11%' AND product_name = '"+Product+"'")
    val N2021 = Averages("2021N")
    val dfQ2_1_21_D = spark.sql (
      "CREATE TEMPORARY VIEW 2021De AS SELECT datetime, qty FROM Orders where datetime like '2021-12%' AND product_name = '"+Product+"'")
    val D2021 = Averages("2021De")
    val Average2021: Seq[(String, Double)] = Seq(("2021-01",J2021),("2021-02",F2021),
      ("2021-03",M2021),("2021-04",A2021),("2021-05",Ma2021),("2021-06",Ju2021),("2021-07",Jy2021),
      ("2021-08",Au2021),("2021-09",S2021),("2021-10",O2021),("2021-11",N2021),
      ("2021-12",D2021))
    Drop()
    Average2021
  }

  def Years(Product: String): Seq[(String, Double)] = {
    val dfQ2_1_21_J = spark.sql (
      "CREATE TEMPORARY VIEW 2021J AS SELECT datetime, qty FROM Orders where datetime like '2022%' AND product_name = '"+Product+"'")
    val  J2021 = Averages("2021J")
    val dfQ2_1_21_F = spark.sql (
      "CREATE TEMPORARY VIEW 2021F AS SELECT datetime, qty FROM Orders where datetime like '2021%' AND product_name = '"+Product+"'")
    val  F2021 = Averages("2021F")
    val dfQ2_1_21_M = spark.sql (
      "CREATE TEMPORARY VIEW 2021M AS SELECT datetime, qty FROM Orders where datetime like '2019%' AND product_name = '"+Product+"'")
    val  M2021 = Averages("2021M")
    val dfQ2_1_21_A = spark.sql (
      "CREATE TEMPORARY VIEW 2021A AS SELECT datetime, qty FROM Orders where datetime like '2018%' AND product_name = '"+Product+"'")
    val  A2021 = Averages("2021A")
    val dfQ2_1_21_Ma = spark.sql (
      "CREATE TEMPORARY VIEW 2021Ma AS SELECT datetime, qty FROM Orders where datetime like '2017%' AND product_name = '"+Product+"'")
    val  Ma2021 = Averages("2021Ma")
    val dfQ2_1_21_Ju = spark.sql (
      "CREATE TEMPORARY VIEW 2021Ju AS SELECT datetime, qty FROM Orders where datetime like '2016%' AND product_name = '"+Product+"'")
    val  Ju2021 = Averages("2021Ju")
    val dfQ2_1_21_Jy = spark.sql (
      "CREATE TEMPORARY VIEW 2021Jy AS SELECT datetime, qty FROM Orders where datetime like '2015%' AND product_name = '"+Product+"'")
    val  Jy2021 = Averages("2021Jy")
    val dfQ2_1_21_Au = spark.sql (
      "CREATE TEMPORARY VIEW 2021Au AS SELECT datetime, qty FROM Orders where datetime like '2014%' AND product_name = '"+Product+"'")
    val  Au2021 = Averages("2021Au")
    val dfQ2_1_21_S = spark.sql (
      "CREATE TEMPORARY VIEW 2021Se AS SELECT datetime, qty FROM Orders where datetime like '2013%' AND product_name = '"+Product+"'")
    val  S2021 = Averages("2021Se")
    val dfQ2_1_21_O = spark.sql (
      "CREATE TEMPORARY VIEW 2021O AS SELECT datetime, qty FROM Orders where datetime like '2012%' AND product_name = '"+Product+"'")
    val  O2021 = Averages("2021O")
    val dfQ2_1_21_N = spark.sql (
      "CREATE TEMPORARY VIEW 2021N AS SELECT datetime, qty FROM Orders where datetime like '2011%' AND product_name = '"+Product+"'")
    val N2021 = Averages("2021N")
    val dfQ2_1_21_D = spark.sql (
      "CREATE TEMPORARY VIEW 2021De AS SELECT datetime, qty FROM Orders where datetime like '2010%' AND product_name = '"+Product+"'")
    val D2021 = Averages("2021De")
    val Average2021: Seq[(String, Double)] = Seq(("2022",J2021),("2021",F2021),
      ("2019",M2021),("2018",A2021),("2017",Ma2021),("2016",Ju2021),("2015",Jy2021),
      ("2014",Au2021),("2013",S2021),("2012",O2021),("2011",N2021),
      ("2010",D2021))
    Drop()
    Average2021
  }

  def Averages(df: String): Double ={
    var D = 0.0
    val dfQ2_1_1 = spark.sql ("Select AVG(qty) from "+df).toDF(df)
    D = GrabValue(dfQ2_1_1, df)
    D
  }

  def GrabValue(df: DataFrame, name: String): Double = {
    var D = 0.0
    val listValues = df.select(name).collect.toList
    if (listValues(0).anyNull) {
      println("No sales this Month")
    } else {
      D = listValues(0).getDouble(0)
    }
    D
  }

  def showAndWriteAllToHTML(): Unit = {
    val pw = new PrintWriter("plots.html")
    if(all_plots.nonEmpty || all_layered_plots.nonEmpty) {
      pw.println(all_plots(0).html.headerHTML())
      if (all_plots.nonEmpty) {
        for (i <- all_plots.indices) {
          all_plots(i).show
          pw.println(all_plots(i).html.plotHTML())
        }
      }
      if (all_layered_plots.nonEmpty) {
        for (i <- all_layered_plots.indices) {
          all_layered_plots(i).show
          pw.println(all_layered_plots(i).html.plotHTML())
        }
      }
      pw.println(all_plots(0).html.footerHTML)
    } else {
      pw.println("No plots to write.")
    }

    pw.close()
  }

  def showAndWriteOneToHTML(plot: ExtendedUnitSpecBuilder, filename: String): Unit = {
    val pw = new PrintWriter(s"$filename.html")
    plot.show
    pw.println(plot.html.pageHTML(filename))
  }
}
