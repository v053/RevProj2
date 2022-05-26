import scala.io.Source
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import java.time.Instant
import java.io.{File, PrintWriter}


object Generator {
  val entries_to_generate = 10000 //10,000 final
  val amt_of_cells: Int = entries_to_generate * 15
  val percent_erroneous = .15 //15%
  val amt_of_errors: Int = (amt_of_cells * percent_erroneous).asInstanceOf[Int]

  var all_customer_IDs: ArrayBuffer[Int] = ArrayBuffer()
  var all_customer_names: ArrayBuffer[String] = ArrayBuffer()
  var all_product_IDs: ArrayBuffer[Int] = ArrayBuffer()
  var all_product_names: ArrayBuffer[String] = ArrayBuffer()
  var all_product_category: ArrayBuffer[String] = ArrayBuffer()
  var all_payment_types: ArrayBuffer[Any] = ArrayBuffer()
  var all_qtys: ArrayBuffer[Int] = ArrayBuffer()
  var all_prices: ArrayBuffer[Double] = ArrayBuffer()
  var all_datetimes: ArrayBuffer[Instant] = ArrayBuffer()
  var all_countries: ArrayBuffer[String] = ArrayBuffer()
  var all_cities: ArrayBuffer[String] = ArrayBuffer()
  var all_website_names: ArrayBuffer[Any] = ArrayBuffer()
  var all_txn_ids: ArrayBuffer[Any] = ArrayBuffer()
  var all_txn_successes: ArrayBuffer[Any] = ArrayBuffer()
  var all_failure_reasons: ArrayBuffer[Any] = ArrayBuffer()

  //---------------------------------------------------------//
  def main(args: Array[String]): Unit = {
    Order()
    //check_arrays()

    error_writer(); //adds some errors 10~15%
    write_csv(); //prints data to .csv
  }
  //end main-------------------------------------------------//

  def check_arrays(): Unit = { // Prints the generated entries to the console
    val all_arrays = Array(
      all_customer_IDs,
      all_customer_names,
      all_product_IDs,
      all_product_names,
      all_product_category,
      all_payment_types,
      all_qtys,
      all_prices,
      all_datetimes,
      all_countries,
      all_cities,
      all_website_names,
      all_txn_ids,
      all_txn_successes,
      all_failure_reasons
    )

    for (i <- 1 to entries_to_generate) {
      for (array <- all_arrays) {
        if (array.nonEmpty) print(s"${array(i)}, ")
      }
      println("")
    }
  }



  def Order(): Unit = {

    for (i <- 0 to entries_to_generate) {
      //Product stuff
      var product_id = 0
      var product_name = ""
      var product_price = 0.00
      var quantity = 0
      var product_cat = ""
      //product category, product id, price
      val Home_supplies = Seq((101,"Window cleaner",6.00), (102,"Mop",12.00),(103,"Box cutter",1.97),
        (104,"Ladder",59.98), (106, "Screws",4.84), (107, "Measuring cup",1.88), (108, "Cutting board", 17.99)
        , (109,"Dinner plates", 0.88), (110, "Ice cube trays", 2.44), (111, "Mixing bowl", 6.45))

      val Tech_supplies = Seq((201,"Laptop",599.99), (202,"Keyboard",19.89), (203, "Mouse", 5.99),
        (204,"Charger",10.99), (205, "Printer",74.00), (206, "Monitor",249.00), (207,"Speaker",25.99),
        (208, "Computer case",58.27), (209, "Phone case", 5.98))

      val School_supplies = Seq((301,"Stapler", 8.97), (302,"Eraser",3.45), (303,"Push-pin",1.14),
        (304, "Thumbtack",10.99), (305,"Paper clip",0.94), (306,"Rubber stamp", 8.99), (307, "Highlighter",0.97),
        (308,"Fountain pen",14.99), (309,"Pencil", 9.98), (310,"Marker", 9.89), (311,"Ballpoint",5.47),
        (312,"Bulldog clip",0.99), (313,"Tape dispenser",4.47), (314,"Pencil sharpener",15.69), (315,"Label",2.12),
        (316,"Calculator",5.62), (317,"Glue",3.79), (318, "Scissors", 8.54), (319,"Sticky notes",11.98), (320,"Paper", 9.72))


      //randomization
      val ram = util.Random

      val ram_category = ram.nextInt(2)
      if(ram_category == 0){
        product_cat = "Home Supplies"
        val Home_supplies1 = ram.nextInt(10)
        //.toString().split(",")
        val temp = Home_supplies(Home_supplies1)
        product_id = temp._1
        product_name = temp._2
        product_price = temp._3
        //println(temp)
      }else if ( ram_category == 1){
        product_cat = "Tech Supplies"
        val tech_supplies1 = ram.nextInt(8)
        val temp = Tech_supplies(tech_supplies1)
        product_id = temp._1
        product_name = temp._2
        product_price = temp._3
        //println(temp)
      }else {
        product_cat = "School Supplies"
        val School_supplies1 = ram.nextInt(19)
        val temp = School_supplies(School_supplies1)
        product_id = temp._1
        product_name = temp._2
        product_price = temp._3
        //println(temp)
      }
      quantity = ram.nextInt(90) + 1
      //println(product_price)


      val arr = readFileToArray("Names.txt")
      val Names = scala.collection.mutable.Map[Int, String]()
      for (n <- 1 to 100) {
        Names += (n -> arr(n - 1))
      }


      val Order_ID = i
      val Cust_ID = Random.nextInt(99) + 1
      //if(Cust_ID !=0) {
      val Name = Names.get(Cust_ID)
      val size = Name.toString.length()
      val string = Name.toString.substring(5, size - 1)
      val temp = string.split(",")
      val Cust_Name = temp(0)
      val Cust_City = temp(1)
      val Cust_Country = temp(2)
      val datetime = randomDate()
      val payment_info = generatePaymentInfo(Order_ID)
      all_customer_IDs += Cust_ID
      all_customer_names += Cust_Name
      all_product_category += product_cat
      all_product_IDs += product_id
      all_product_names += product_name
      all_payment_types += payment_info(1)
      all_qtys += quantity
      all_prices += product_price
      all_datetimes += datetime
      all_countries += Cust_Country
      all_cities += Cust_City
      all_website_names += payment_info(4)
      all_txn_ids += payment_info.head
      all_txn_successes += payment_info(2)
      all_failure_reasons += payment_info(3)
      //}
    }
  }

  def selectRandomElement(array: Array[_]): Any = {
    val length = array.length
    val result = array(Random.nextInt(length))
    result
  }

  def generatePaymentInfo(Order_ID: Int): List[Any] = {
    val payment_processed_options = Array("Y", "N")
    val reasons_for_failure = Array("Transaction limit exceeded", "Invalid billing address", "Invalid payment method", "System failure", "Unknown")
    val payment_type_options = Array("card", "Internet Banking", "UPI", "Wallet")
    val websites = readFileToArray("websites.txt")
    var failure_reason: Any = "N/A"

    val website = selectRandomElement(websites)
    val payment_txn_id = Order_ID
    val payment_txn_success = selectRandomElement(payment_processed_options)
    val payment_type = selectRandomElement(payment_type_options)

    if (payment_txn_success == "N") {
      failure_reason = selectRandomElement(reasons_for_failure)
    }
    // println(payment_txn_id.toString + "," + payment_type + "," + payment_txn_success + "," + failure_reason + "," + website)

    List(payment_txn_id, payment_type, payment_txn_success, failure_reason, website)
  }

  // https://alvinalexander.com/source-code/scala-function-read-text-file-into-array-list/
  def readFileToArray(filename: String): Array[String] = {
    //val f = new File(getClass.getClassLoader.getResource(filename).getPath)
    val f = s"C:\\Users\\Erienne Work\\Documents\\Revature\\Training Projects\\Project2\\src\\main\\resources\\$filename"
    val file = Source.fromFile(f)
    val lines = file.getLines.toArray
    file.close()
    lines
  }

  // https://stackoverflow.com/questions/35774504/random-date-between-2-given-dates
  // https://www.baeldung.com/java-random-dates
  def randomDate(): Instant = {
    val start = Instant.parse("2010-01-01T00:00:00.001Z")
    val start_long = start.getEpochSecond
    val current = Instant.now
    val current_long: Long = current.getEpochSecond
    val max_int = (current_long-start_long).toInt
    val random_int = Random.nextInt(max_int-1) + 1
    val random_long = random_int.toLong
    val time = start.plusSeconds(random_long)
    time
  }

  def error_writer(): Unit = {
    //println(amt_of_errors);
    val lo = 0; //start -0
    val hi = all_customer_names.length - 1; //total num of entries (rows)

    val r = new scala.util.Random; //creates random number instance
    var rn = lo + r.nextInt((hi - lo) + 1) - 1; //init random num
    var rn2 = lo + r.nextInt((15 - lo) + 1) - 1; //which field in that row

    for (i <- 0 until amt_of_errors) { //for the amount of errors we wanna gen

      rn = lo + r.nextInt((hi - lo) + 1); //picks a random row
      rn2 = lo + r.nextInt((15 - lo) + 1); //pick column
      if (rn2 == 0) {
        all_customer_IDs(rn) = -1

      } else if (rn2 == 1) {
        all_customer_names(rn) = null

      } else if (rn2 == 2) {
        all_product_IDs(rn) = -1

      } else if (rn2 == 3) {
        all_product_names(rn) = null

      } else if (rn2 == 4) {
        all_product_category(rn) = null

      } else if (rn2 == 5) {
        all_payment_types(rn) = null

      } else if (rn2 == 6) {
        all_qtys(rn) = null

      } else if (rn2 == 7) {
        all_prices(rn) = null

      } else if (rn2 == 8) {
        all_datetimes(rn) = null

      } else if (rn2 == 9) {
        all_countries(rn) = null

      } else if (rn2 == 10) {
        all_cities(rn) = null

      } else if (rn2 == 11) {
        all_website_names(rn) = null

      } else if (rn2 == 12) {
        all_txn_ids(rn) = null

      } else if (rn2 == 13) {
        all_txn_successes(rn) = null

      } else if (rn2 == 14) {
        all_failure_reasons(rn) = null
      }
    }

  }

  def write_csv(): Unit = {
    val f = new File(getClass.getClassLoader.getResource("data.csv").getPath)
    val pw = new PrintWriter("data.csv")

    //def write_csv(): Unit = {

      //val pw = new PrintWriter("data.csv")

      for (i <- 1 until all_customer_IDs.length) {
        pw.print(i+",")
        pw.print(s"${all_customer_IDs(i)},") //customer ID
        pw.print(s"${all_customer_names(i)},") //customer names
        pw.print(s"${all_product_IDs(i)},")           //product ID
        pw.print(s"${all_product_names(i)},")      //product names
        pw.print(s"${all_product_category(i)},")     //product category
        pw.print(s"${all_payment_types(i)},") //payment type
        pw.print(s"${all_qtys(i)},")     //qtys
        //println(all_qtys(i))
        pw.print(s"${all_prices(i)},")     //prices
        //println(all_prices(i))
        pw.print(s"${all_datetimes(i)},") //date times
        pw.print(s"${all_countries(i)},") //country
        pw.print(s"${all_cities(i)},") //cities
        pw.print(s"${all_website_names(i)},") //website names
        pw.print(s"${all_txn_ids(i)},") //txn IDs
        pw.print(s"${all_txn_successes(i)},") //txn success
        pw.println(s"${all_failure_reasons(i)},") //failure reasons
      }

      pw.close() //always close to prevent seg fault
   // }
  }
}

