import scala.io.Source
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import java.time.Instant

object Generator {
  val entries_to_generate = 10

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

  def main(args: Array[String]): Unit = {
    Order()
    check_arrays()
  }

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

    for(i <- 0 until entries_to_generate) {
      for(array <- all_arrays) {
        if (array.nonEmpty) print(array(i) + ", ")
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

    for(i <- 1 to entries_to_generate) {
      val Order_ID = i
      val Cust_ID = Random.nextInt(100)
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
    val bufferedSource = Source.fromFile(s"src\\main\\resources\\$filename")
    val lines = (for (line <- bufferedSource.getLines()) yield line).toArray
    bufferedSource.close
    lines
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
}

