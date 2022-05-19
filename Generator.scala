package Project2
import scala.io.Source
import Array._

object Generator {
  def main(args: Array[String]): Unit = {
      Order()
  }

  def Order(): Array[String] =
    {
      var Customer = Array.empty[String]
      val file = "C:\\Users\\ethan\\IdeaProjects\\HelloScala\\src\\main\\scala\\Project2\\Names.txt"
      val arr = Source.fromFile(file).getLines().toArray
      val Names = scala.collection.mutable.Map[Int, String]()
      for(n <- 1 to 100){
        Names += (n-> arr(n-1))
      }
      val r = scala.util.Random
      for(i <- 1 to 10) {
        val Order_ID = i
        val Cust_ID = r.nextInt(100)
        val Name = Names.get(Cust_ID)
        val size = Name.toString().length()
        val string = Name.toString().substring(5, size - 1)
        val temp = string.split(",")
        val Cust_Name = temp(0)
        var Cust_City = temp(1)
        var Cust_Country = temp(2)
        val str = i+","+Cust_ID+","+Cust_Name+","+Cust_City+","+Cust_Country
        Customer = Customer :+ str
      }
      return Customer
    }
}
