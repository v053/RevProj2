import scala.io.Source
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import java.time.Instant
import java.io.{File, PrintWriter}


object Generator {
  val entries_to_generate = 10000  //10,000 final
  val amt_of_cells = entries_to_generate * 15
  val percent_erroneous = .15   //15%
  val amt_of_errors = (amt_of_cells * percent_erroneous).asInstanceOf[Int]

  var all_customer_IDs: ArrayBuffer[Int] = ArrayBuffer()
  var all_customer_names: ArrayBuffer[String] = ArrayBuffer()
  var all_product_IDs: ArrayBuffer[Int] = ArrayBuffer()
  var all_product_names: ArrayBuffer[String] = ArrayBuffer()
  var all_product_category: ArrayBuffer[Int] = ArrayBuffer()
  var all_payment_types: ArrayBuffer[Any] = ArrayBuffer()
  var all_qtys: ArrayBuffer[Int] = ArrayBuffer()
  var all_prices: ArrayBuffer[Int] = ArrayBuffer()
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

    error_writer();   //adds some errors 10~15%
    write_csv();      //prints data to .csv
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

    for(i <- 1 until entries_to_generate) {
      for(array <- all_arrays) {
        if (array.nonEmpty) print(s"${array(i)}, ")
      }
      println("")
    }
  }

  def Order(): Unit = {
    val arr = readFileToArray("Names.txt")
    val Names = scala.collection.mutable.Map[Int, String]()
    for(n <- 1 to 100){
      Names += (n-> arr(n-1))
    }

    for(i <- 0 to (entries_to_generate - 1)) {
      val Order_ID = i
      val Cust_ID = Random.nextInt(99)+1
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
        // all_product_IDs += product_id
        // all_product_names += product_name
        all_payment_types += payment_info(1)
        // all_qtys
        // all_prices
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
    val payment_processed_options = Array("Y","N")
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
    val f = new File(getClass.getClassLoader.getResource(filename).getPath)
    val lines = Source.fromFile(f).getLines.toArray
    lines
  }

  // https://stackoverflow.com/questions/35774504/random-date-between-2-given-dates
  // https://www.baeldung.com/java-random-dates
  def randomDate(): Instant = {
    val start: Long = 946684800000L
    val current = Instant.now().toEpochMilli
    //(current-start)+1
    val random = Random.nextLong()
    val time = Instant.ofEpochMilli(start+random)
    time
  }

  def error_writer( ) = {
    //println(amt_of_errors);
    val lo = 0;                               //start -0
    val hi = all_customer_names.length - 1;   //total num of entries (rows)

    val r = new scala.util.Random;            //creates random number instance
    var rn = lo + r.nextInt((hi - lo) + 1) - 1;   //init random num
    var rn2 = lo + r.nextInt((15 - lo) + 1) - 1;  //which field in that row

    for(i <- 0 to (amt_of_errors - 1)) {            //for the amount of errors we wanna gen

      rn = lo + r.nextInt((hi - lo) + 1);     //picks a random row
      rn2 = lo + r.nextInt((15 - lo) + 1);    //pick column
      println(rn)
      if(rn2 == 0){
        all_customer_IDs(rn) = -1;

      }else if(rn2 == 1){
        all_customer_names(rn) = null;

      }else if(rn2 == 2){
        //all_product_IDs(rn) = -1;

      }else if(rn2 == 3){
        //all_product_names(rn) = null;

      }else if(rn2 == 4){
        //all_product_category(rn) = null;

      }else if(rn2 == 5){
        all_payment_types(rn) = null;

      }else if(rn2 == 6){
        //all_qtys(rn) = null;

      }else if(rn2 == 7){
        //all_prices(rn) = null;

      }else if(rn2 == 8){
        all_datetimes(rn) = null;

      }else if(rn2 == 9){
        all_countries(rn) = null;

      }else if(rn2 == 10){
        all_cities(rn) = null;

      }else if(rn2 == 11){
        all_website_names(rn) = null;

      }else if(rn2 == 12 ){
        all_txn_ids(rn) = null;

      }else if(rn2 == 13){
        all_txn_successes(rn) = null;

      }else if(rn2 == 14){
        all_failure_reasons(rn) = null;
      }
    }

  }

  def write_csv( ) = {
    val f = new File(getClass.getClassLoader.getResource("data.csv").getPath)
    val pw = new PrintWriter("data.csv")

    for(i <- 1 until all_customer_names.length) {
      pw.print(i+",")
      pw.print(all_customer_IDs(i)+",")     //customer ID
      pw.print(s"${all_customer_names(i)},")      //customer names
      //pw.print(s"${all_product_IDs(i)},");           //product ID
      //pw.print(s"${all_product_names(i)},");      //product names
      //pw.print(s"${all_product_category(i)},");     //product category
      pw.print(s"${all_payment_types(i)},")     //payment type
      //pw.print(s"${all_qtys(i)},");     //qtys
      //pw.print(s"${all_prices(i)},");     //prices
      pw.print(s"${all_datetimes(i)},");     //date times
      pw.print(s"${all_countries(i)},");     //country
      pw.print(s"${all_cities(i)},");     //cities
      pw.print(s"${all_website_names(i)},");     //website names
      pw.print(s"${all_txn_ids(i)},");     //txn IDs
      pw.print(s"${all_txn_successes(i)},");     //txn success
      pw.println(s"${all_failure_reasons(i)}"); //failure reasons
    }

    pw.close;   //always close to prevent seg fault
  }
}
