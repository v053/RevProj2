
import Generator.getClass
import vegas._
import vegas.sparkExt._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import vegas.DSL.UnitSpecBuilder

import java.io.File
import scala.collection.mutable.ArrayBuffer


object DataVisualizer {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark-Vegas Data Visualizer")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()

  val df = spark.read.csv("data.csv")
  df.createOrReplaceTempView("Orders")
  df.registerTempTable("Orders")
 // val dfQ2_1_21_J = spark.sql (
 //   "SELECT _c9, _c7, _c4 FROM Orders where _c9 like '2021-01%'").toDF("Time","Products Sold","Product Name")
 // dfQ2_1_21_J.show()
  val dfQ1_1 = spark.sql("SELECT _c5, sum(_c7) FROM Orders group by _c5").toDF("Category","Products Sold")
  //dfQ1_1.show()
  val dfQ1_2 = spark.sql("SELECT _c11, _c5, sum(_c7) FROM Orders group by _c11, _c5").toDF("Country","Category","Products Sold")
  //dfQ1_2.show()

  val sample_data1: Seq[(String, Int)] = Seq(("stationery", 10), ("household products", 3), ("groceries", 5))
  val sample_data2: Seq[(String, Int)] = Seq(("00:00",50), ("01:00", 120), ("02:00",150), ("03:00",400))
  val sample_data3: Seq[(String, Int)] = Seq(("00:00",20), ("01:00", 100), ("02:00",350), ("03:00",470))
  val sample_data4: Seq[(String, Int)] = Seq(("00:00",30), ("01:00", 110), ("02:00",250), ("03:00",450))
  val sample_df1: DataFrame = spark.createDataFrame(sample_data1).toDF("Category Name", "Products Sold")
  val sample_df2: DataFrame = spark.createDataFrame(sample_data2).toDF("Time", "Access")
  val sample_df3: DataFrame = spark.createDataFrame(sample_data3).toDF("Time", "Access")


  def main(args: Array[String]): Unit = {
    Question2()
    //plotBarChart(dfQ1_2)
    //plotLineChart(dfQ2_1_21_J)
   // plotLineChart(dfQ2_1_2)
    //plotMultiLineChart(Array(sample_df3,sample_df2))
    //plotMultiLineChart(Array(dfQ2_1_1,dfQ2_1_2))
  }
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

  def plotBarChart(df: DataFrame, chart_title: String = ""): Unit = {
    val col_name_array = df.columns
    val data_type_array: ArrayBuffer[spec.Spec.Type] = ArrayBuffer()

    for (column <- col_name_array) {
      val my_data_type = df.schema(column).dataType
      my_data_type match {
        case StringType => data_type_array += Nom
        case IntegerType => data_type_array += Quant
        case DoubleType => data_type_array += Quant
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

      plot.show
    } else {
      val plot = Vegas(chart_title)
        .withDataFrame(df)
        .encodeX(col_name_array(0), data_type_array(0))
        .encodeY(col_name_array(1), data_type_array(1))
        .mark(Line)

      plot.show
    }
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
        case DoubleType => data_type_array += Quant
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
      "CREATE TEMPORARY VIEW 2021J AS SELECT _c9, _c7 FROM Orders where _c9 like '2021-01%' AND _c4 = '"+Product+"'")
    val  J2021 = Averages("2021J")
    val dfQ2_1_21_F = spark.sql (
      "CREATE TEMPORARY VIEW 2021F AS SELECT _c9, _c7 FROM Orders where _c9 like '2021-02%' AND _c4 = '"+Product+"'")
    val  F2021 = Averages("2021F")
    val dfQ2_1_21_M = spark.sql (
      "CREATE TEMPORARY VIEW 2021M AS SELECT _c9, _c7 FROM Orders where _c9 like '2021-03%' AND _c4 = '"+Product+"'")
    val  M2021 = Averages("2021M")
    val dfQ2_1_21_A = spark.sql (
      "CREATE TEMPORARY VIEW 2021A AS SELECT _c9, _c7 FROM Orders where _c9 like '2021-04%' AND _c4 = '"+Product+"'")
    val  A2021 = Averages("2021A")
    val dfQ2_1_21_Ma = spark.sql (
      "CREATE TEMPORARY VIEW 2021Ma AS SELECT _c9, _c7 FROM Orders where _c9 like '2021-05%' AND _c4 = '"+Product+"'")
    val  Ma2021 = Averages("2021Ma")
    val dfQ2_1_21_Ju = spark.sql (
      "CREATE TEMPORARY VIEW 2021Ju AS SELECT _c9, _c7 FROM Orders where _c9 like '2021-06%' AND _c4 = '"+Product+"'")
    val  Ju2021 = Averages("2021Ju")
    val dfQ2_1_21_Jy = spark.sql (
      "CREATE TEMPORARY VIEW 2021Jy AS SELECT _c9, _c7 FROM Orders where _c9 like '2021-07%' AND _c4 = '"+Product+"'")
    val  Jy2021 = Averages("2021Jy")
    val dfQ2_1_21_Au = spark.sql (
      "CREATE TEMPORARY VIEW 2021Au AS SELECT _c9, _c7 FROM Orders where _c9 like '2021-08%' AND _c4 = '"+Product+"'")
    val  Au2021 = Averages("2021Au")
    val dfQ2_1_21_S = spark.sql (
      "CREATE TEMPORARY VIEW 2021Se AS SELECT _c9, _c7 FROM Orders where _c9 like '2021-09%' AND _c4 = '"+Product+"'")
    val  S2021 = Averages("2021Se")
    val dfQ2_1_21_O = spark.sql (
      "CREATE TEMPORARY VIEW 2021O AS SELECT _c9, _c7 FROM Orders where _c9 like '2021-10%' AND _c4 = '"+Product+"'")
    val  O2021 = Averages("2021O")
    val dfQ2_1_21_N = spark.sql (
      "CREATE TEMPORARY VIEW 2021N AS SELECT _c9, _c7 FROM Orders where _c9 like '2021-11%' AND _c4 = '"+Product+"'")
    val N2021 = Averages("2021N")
    val dfQ2_1_21_D = spark.sql (
      "CREATE TEMPORARY VIEW 2021De AS SELECT _c9, _c7 FROM Orders where _c9 like '2021-12%' AND _c4 = '"+Product+"'")
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
      "CREATE TEMPORARY VIEW 2021J AS SELECT _c9, _c7 FROM Orders where _c9 like '2022%' AND _c4 = '"+Product+"'")
    val  J2021 = Averages("2021J")
    val dfQ2_1_21_F = spark.sql (
      "CREATE TEMPORARY VIEW 2021F AS SELECT _c9, _c7 FROM Orders where _c9 like '2021%' AND _c4 = '"+Product+"'")
    val  F2021 = Averages("2021F")
    val dfQ2_1_21_M = spark.sql (
      "CREATE TEMPORARY VIEW 2021M AS SELECT _c9, _c7 FROM Orders where _c9 like '2019%' AND _c4 = '"+Product+"'")
    val  M2021 = Averages("2021M")
    val dfQ2_1_21_A = spark.sql (
      "CREATE TEMPORARY VIEW 2021A AS SELECT _c9, _c7 FROM Orders where _c9 like '2018%' AND _c4 = '"+Product+"'")
    val  A2021 = Averages("2021A")
    val dfQ2_1_21_Ma = spark.sql (
      "CREATE TEMPORARY VIEW 2021Ma AS SELECT _c9, _c7 FROM Orders where _c9 like '2017%' AND _c4 = '"+Product+"'")
    val  Ma2021 = Averages("2021Ma")
    val dfQ2_1_21_Ju = spark.sql (
      "CREATE TEMPORARY VIEW 2021Ju AS SELECT _c9, _c7 FROM Orders where _c9 like '2016%' AND _c4 = '"+Product+"'")
    val  Ju2021 = Averages("2021Ju")
    val dfQ2_1_21_Jy = spark.sql (
      "CREATE TEMPORARY VIEW 2021Jy AS SELECT _c9, _c7 FROM Orders where _c9 like '2015%' AND _c4 = '"+Product+"'")
    val  Jy2021 = Averages("2021Jy")
    val dfQ2_1_21_Au = spark.sql (
      "CREATE TEMPORARY VIEW 2021Au AS SELECT _c9, _c7 FROM Orders where _c9 like '2014%' AND _c4 = '"+Product+"'")
    val  Au2021 = Averages("2021Au")
    val dfQ2_1_21_S = spark.sql (
      "CREATE TEMPORARY VIEW 2021Se AS SELECT _c9, _c7 FROM Orders where _c9 like '2013%' AND _c4 = '"+Product+"'")
    val  S2021 = Averages("2021Se")
    val dfQ2_1_21_O = spark.sql (
      "CREATE TEMPORARY VIEW 2021O AS SELECT _c9, _c7 FROM Orders where _c9 like '2012%' AND _c4 = '"+Product+"'")
    val  O2021 = Averages("2021O")
    val dfQ2_1_21_N = spark.sql (
      "CREATE TEMPORARY VIEW 2021N AS SELECT _c9, _c7 FROM Orders where _c9 like '2011%' AND _c4 = '"+Product+"'")
    val N2021 = Averages("2021N")
    val dfQ2_1_21_D = spark.sql (
      "CREATE TEMPORARY VIEW 2021De AS SELECT _c9, _c7 FROM Orders where _c9 like '2010%' AND _c4 = '"+Product+"'")
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
    val dfQ2_1_1 = spark.sql ("Select AVG(_C7) from "+df).toDF(df)
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
}