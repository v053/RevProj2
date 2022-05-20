import scala.io.Source
import scala.util.Random
import java.time.Instant

object Generator {
  def main(args: Array[String]): Unit = {

    val arr = readFileToArray("Names.txt")
    val Names = scala.collection.mutable.Map[Int, String]()
    for (n <- 1 to 100) {
      Names += (n -> arr(n - 1))
    }

    val r = scala.util.Random
    for (i <- 1 to 10) {
      var Order_ID = i
      var Cust_ID = r.nextInt(100)
      var Name = Names.get(Cust_ID)
      var size = Name.toString.length()
      var Cust_Name = Name.toString.substring(5, size - 1)
      var temp = Cust_Name.split(",")
      var Cust_City = temp(1)
      var Cust_Country = temp(2)
      println(Order_ID + "," + Cust_ID + "," + Cust_Name)


      val datetime = randomDate()
      println(datetime)

      val payment_info_array = generatePaymentInfo(Order_ID)
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
    println(payment_txn_id.toString + "," + payment_type + "," + payment_txn_success + "," + failure_reason + "," + website)

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

