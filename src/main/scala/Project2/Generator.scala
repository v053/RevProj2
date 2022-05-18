package Project2
import scala.io.Source

object Generator {
  def main(args: Array[String]): Unit = {

    var file = "C:\\Users\\ethan\\IdeaProjects\\HelloScala\\src\\main\\scala\\Project2\\Names.txt"
    val arr = Source.fromFile(file).getLines().toArray
    val Names = scala.collection.mutable.Map[Int, String]()
    for(n <- 1 to 100){
      Names += (n-> arr(n-1))
    }
    val r = scala.util.Random
    for(i <- 1 to 10){
      var Order_ID = i
      var Cust_ID = r.nextInt(100)
      var Name = Names.get(Cust_ID)
      var size  = Name.toString().length()
      var Cust_Name = Name.toString().substring(5,size-1)
      var temp = Cust_Name.split(",")
      var Cust_City = temp(1)
      var Cust_Country = temp(2)
      println(Order_ID+","+Cust_ID+","+Cust_Name)
    }
  }
}
