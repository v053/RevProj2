import scala.io.Source
import scala.util.Random

object Generator {
  val test_array: Array[String] = Array("Yes", "No")

  def main(args: Array[String]): Unit = {

    var file = "C:\\Users\\Erienne Work\\Documents\\Revature\\Training Projects\\Project2\\Names.txt"
    val arr = Source.fromFile(file).getLines().toArray
    val Names = scala.collection.mutable.Map[Int, String]()
    for (n <- 1 to 100) {
      Names += (n -> arr(n - 1))
    }

    val r = scala.util.Random
    for (i <- 1 to 10) {
      var Order_ID = i
      var Cust_ID = r.nextInt(100)
      var Name = Names.get(Cust_ID)
      var size = Name.toString().length()
      var Cust_Name = Name.toString().substring(5, size - 1)
      var temp = Cust_Name.split(",")
      var Cust_City = temp(1)
      var Cust_Country = temp(2)
      println(Order_ID + "," + Cust_ID + "," + Cust_Name)
      generatePaymentInfo(Order_ID)
    }
  }

  def selectRandomElement(array: Array[_]): Any = {
    val length = array.length
    val result = array(Random.nextInt(length))
    result
  }

  def generatePaymentInfo(Order_ID: Int): Unit = {
    val payment_processed_options = Array("Y","N")
    val reasons_for_failure = Array("Transaction limit exceeded", "Invalid billing address", "Invalid payment method", "System failure", "Unknown")
    var failure_reason: Any = ""

    val payment_txn_id = Order_ID
    val payment_txn_success = selectRandomElement(payment_processed_options)

    if (payment_txn_success == "N") {
      failure_reason = selectRandomElement(reasons_for_failure)
    }
    println(payment_txn_id.toString + "," + payment_txn_success + "," + failure_reason)
  }
}
