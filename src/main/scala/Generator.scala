import scala.io.Source
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import java.time.Instant
import java.io.{PrintWriter, File}


object Generator {
  val entries_to_generate = 10000

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

    //erroneous( );   //adds some errors 10~15% 
    write_csv();   //prints data to .csv
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

    for(i <- 1 to entries_to_generate) {
      for(array <- all_arrays) {
        if (array.nonEmpty) print(array(i) + ", ")
      }
      println("")
    }
  }

  def Order(): Unit = {
    // val arr = readFileToArray("Names.txt")
    val arr = Source.fromFile("C:\\Users\\Erienne Work\\Documents\\Revature\\Training Projects\\Project2\\src\\main\\resources\\Names.txt").getLines.toArray
    val Names = scala.collection.mutable.Map[Int, String]()
    for(n <- 1 to 100){
      Names += (n-> arr(n-1))
    }

    for(i <- 0 to entries_to_generate) {
      val Order_ID = i
      val Cust_ID = (Random.nextInt(99)+1)
      if(Cust_ID != 0) {
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
      }
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
    val websites = Source.fromFile("C:\\Users\\Erienne Work\\Documents\\Revature\\Training Projects\\Project2\\src\\main\\resources\\websites.txt").getLines.toArray
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
    //val lines = Source.fromFile(f).getLines.toArray
    /*val bufferedSource = Source.fromFile(f)
    val bufferedSource = Source.fromFile(s"../resources/$filename")
    val lines = (for (line <- bufferedSource.getLines()) yield line).toArray
    bufferedSource.close*/
    //lines
    Array("It's fine")
  }

  // https://stackoverflow.com/questions/35774504/random-date-between-2-given-dates
  // https://www.baeldung.com/java-random-dates
  def randomDate(): Instant = {
    val start: Long = 946684800000L
    val current = Instant.now().toEpochMilli
    val random = Random.nextLong((current-start)+1)
    val time = Instant.ofEpochMilli(start+random)
    time
  }

  def write_csv( ) = {
    
    val pw = new PrintWriter("data.csv")

    for(i <- 1 until all_customer_IDs.length) {
      pw.print(s"$i,")
      pw.print(s"${all_customer_IDs(i)},");      //customer ID
      pw.print(s"${all_customer_names(i)},");      //customer names
      //pw.print(s"${all_product_IDs(i)},");           //product ID
      //pw.print(s"${all_product_names(i)},");      //product names 
      //pw.print(s"${all_product_category(i)},");     //product category
      pw.print(s"${all_payment_types(i)},");     //payment type 
      //pw.print(s"${all_qtys(i)},");     //qtys
      //pw.print(s"${all_prices(i)},");     //prices
      pw.print(s"${all_datetimes(i)},");     //date times
      pw.print(s"${all_countries(i)},");     //country
      pw.print(s"${all_cities(i)},");     //cities
      pw.print(s"${all_website_names(i)},");     //website names
      pw.print(s"${all_txn_ids(i)},");     //txn IDs
      pw.print(s"${all_txn_successes(i)},");     //txn success
      pw.println(s"${all_failure_reasons(i)},");     //failure reasons
    }

    pw.close;   //always close to prevent seg fault 
  }
}

