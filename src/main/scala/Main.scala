import scala.util.control.Breaks._
import DataVisualizer.{df, spark}
import org.apache.spark.sql.SparkSession

import java.io.{File, FileOutputStream, FileWriter, PrintWriter}
import scala.io.Source
import scala.util.control.Breaks.{getClass, _}
import sys.exit

//TODO: local imports for what's needed
//For data visualization,
//Generator?, (omitted atm)
//from exterrior query files (if functionality not includedd here)


object Main {
  //val f = new File(getClass.getClassLoader.getResource("Login.txt").getPath)
  val f = Source.fromFile("Login.txt")
  //val f = "C:\\Users\\Erienne Work\\Documents\\Revature\\Training Projects\\Project2\\src\\main\\resources\\Login.txt"
  val lines = f.getLines.toArray
  val users = scala.collection.mutable.Map[String, String]()
  for (n <- 0 until lines.length) {
    val temp = lines(n).split(",")
    users += (temp(0) -> temp(1))
  }
  //-----------------------------------------------//
  def main(args:Array[String]): Unit = {
    Login(users)
  }

  def selection(cmd: String): Unit = {
    var exit : Boolean = false;   //exit program flag
    breakable{
      while(exit == false) {
        if(exit == true) {
          break();

          //}else if(cmd == "-g") {   //re-generate data
          //  //TODO?: call to generate.scala

        }else if(cmd == "-v") {   //visualization tools
          //TODO: call data visualization
          menu_main();

        }else if(cmd == "-q1") {
          //TODO: calls to query 1 functionality here
          DataVisualizer.queryTopSellingProduct()
          DataVisualizer.queryTopSellingProductByCountry()
          menu_main();

        }else if(cmd == "-q2") {
          //TODO: calls to query 2 functionality here
          DataVisualizer.Question2()
          menu_main();

        }else if(cmd == "-q3") {
          //TODO: calls to query 3 functionality here
          DataVisualizer.queryHighestTrafficOfSales()
          menu_main();

        }else if(cmd == "-q4") {
          //TODO: calls to query 4 functionality here
          DataVisualizer.Q4()
          menu_main();

        }else if(cmd == "-h") {   //help for more options
          menu_help();
          menu_main();

        }else if(cmd == "-e") {   //exit
          exit = true;
          sys.exit()
          break();
        }
      }

    }
  }
  //-----------------------------------------------//

  def menu_main(): Unit = {
    println("-------------------------------------------");
    println("                 Main Menu                 ");
    println("-------------------------------------------");
    println("");
    println("Input one of the following options:  ");
    println(" ");
    //println("-g");
    println("-v (run data visualization tools)");
    println(" ");
    println("-q1 (Top selling product categories)");
    println("-q2 (Popularity of products throughout year)");
    println("-q3 (Locations with highest traffic of sales)");
    println("-q4 (Times with highest traffic of sales)");
    println(" ");
    println("-h (help for more options)");
    println("-e (exit)");
    println(" ");
    println("Please input an option: ");

    var op : String = " ";

    do {
      op = scala.io.StdIn.readLine( );
      if(!(op == "-v" || op == "-q1" || op == "-q2" || op == "-q3" || op == "-q4" || op == "-h" || op == "-e")) {
        println("Error! Please input a valid menu option.");
        println("Or, enter '-h' for more details.");
      }
    }while((op != "-v" && op != "-q1" && op != "-q2" && op != "-q3" && op != "-q4" && op != "-h" && op != "-e"));

    selection(op);
  }

  def menu_help(): Unit = {
    println("------------------------------------------");
    println("                 Help Menu:               ");
    println("------------------------------------------");
    println("");
    //println("-g  : generate random data set into /resources/data.csv");
    println("-v  : "); //TODO: Write visualization description
    println("-q1 : "); //TODO: descriptions below ..
    println("-q2 : "); //
    println("-q3 : "); //
    println("-q4 : "); //
    println("-e  : exit program");
    println("------------------------------------------");

  }

  def Login(users: scala.collection.mutable.Map[String, String]): Unit = {
    var cmd = "-e"

    println("------------------------------------------")
    println("                  Login:                  ")
    println("------------------------------------------")
    println("Type in signup to sign up")
    println("Username: ");
    var user : String = " "
    user = scala.io.StdIn.readLine( )
    if(user == "signup"){
      Signup(users)
    }
    println("Password: ")
    var pass : String = " "
    pass = scala.io.StdIn.readLine()
    if(users.contains(user)) {
      val Passw = users.get(user)
      val Password = Passw.toString.substring(5, Passw.toString.length - 1)
      if (Password == pass) {
        println("Login Succesfull")
        menu_main()
      } else {
        println("Password or username incorrect")
        Login(users)
      }
    }else{
      println("Password or username incorrect")
      Login(users)
    }
    selection(cmd)
  }

  def Signup(users: scala.collection.mutable.Map[String, String]): Unit ={
    println("------------------------------------------")
    println("                 SignUp:                  ")
    println("------------------------------------------")
    println("Type in login to Login")
    println("Username: ");
    var user : String = " "
    user = scala.io.StdIn.readLine( )
    if(user == "login"){
      Login(users)
    }

    if(users.contains(user)){
      println("User already exists")
      Signup(users)
    }
    // val UserSearch = spark.sql("SELECT * from Login where User = '"+user+"'")= '"+user+"'"
    var i = 1
    while(i == 1) {
      println("Password: ");
      var pass: String = " "
      pass = scala.io.StdIn.readLine()
      println("Reenter Password: ")
      val temp = pass
      pass = scala.io.StdIn.readLine()
      if (temp == pass) {
        //val f = new File(getClass.getClassLoader.getResource("Login.txt").getPath)
        val fw = new FileWriter("Login.txt", true)
        //val pw = new PrintWriter(new File("Login.txt"))
        try {
          fw.write(user+","+pass)
          fw.write("\n")
        }
        finally fw.close()
        users += (user -> pass)
        i = 0
      } else {
        println("Passwords don't match")
      }
    }



    Login(users)
  }

}