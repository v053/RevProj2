import DataVisualizer.{df, spark}
import org.apache.spark.sql.SparkSession

import java.io.{File, FileWriter}
import scala.io.Source
import scala.util.control.Breaks.{getClass, _}

//TODO: local imports for what's needed
//For data visualization,
//Generator?, (omitted atm)
//from exterrior query files (if functionality not includedd here)


object Main {
  val f = new File(getClass.getClassLoader.getResource("Login.txt").getPath)
  val lines = Source.fromFile(f).getLines.toArray
  val users = scala.collection.mutable.Map[String, String]()
  for (n <- 0 until lines.length) {
    val temp = lines(n).split(",")
    users += (temp(0) -> temp(1))
  }
  //-----------------------------------------------//
  def main(args:Array[String]): Unit = {

    var exit : Boolean = false;   //exit program flag
    var cmd : String = " ";       //active input read

    //cmd = menu_main();            //inital menu print / choose option

    cmd = Login(users)

    breakable{
      while(exit == false) {

        if(exit == true) {
          break();

          //}else if(cmd == "-g") {   //re-generate data
          //  //TODO?: call to generate.scala

        }else if(cmd == "-v") {   //visualization tools
          //TODO: call data visualization
          cmd = menu_main();

        }else if(cmd == "-q1") {
          //TODO: calls to query 1 functionality here
          cmd = menu_main();

        }else if(cmd == "-q2") {
          //TODO: calls to query 2 functionality here
          cmd = menu_main();

        }else if(cmd == "-q3") {
          //TODO: calls to query 3 functionality here
          cmd = menu_main();

        }else if(cmd == "-q4") {
          //TODO: calls to query 4 functionality here
          cmd = menu_main();

        }else if(cmd == "-h") {   //help for more options
          menu_help();
          cmd = menu_main();

        }else if(cmd == "-e") {   //exit
          exit = true;
          break();
        }

      }
    }

  }
  //-----------------------------------------------//

  def menu_main(): String = {
    println("-------------------------------------------");
    println("                 Main Menu                 ");
    println("-------------------------------------------");
    println("");
    println("Input one of the following options:  ");
    println(" ");
    //println("-g");
    println("-v (run data visualization tools)");
    println(" ");
    println("-q1 ()");
    println("-q2 ()");
    println("-q3 ()");
    println("-q4 ()");
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

    return(op);
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

  def Login(users: scala.collection.mutable.Map[String, String]): String = {
    var cmd = "-e"
  /*  val spark: SparkSession = SparkSession
      .builder()
      .appName("Spark-Vegas Data Visualizer")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
*/

 //   val df = spark.read.csv("src\\main\\resources\\login.csv").toDF("User","Password")
  //  df.createOrReplaceTempView("Login")
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
   // val UserSearch = spark.sql("SELECT * from Login where User = '"+user+"'")= '"+user+"'"
    println("Password: ");
    var pass : String = " "
    pass = scala.io.StdIn.readLine()
    //val PassSearch = spark.sql("SELECT * from Login where Password = '"+pass+"' and User = '"+user+"'")
    if(users.contains(user)) {
      val Passw = users.get(user)
      val Password = Passw.toString.substring(5, Passw.toString.length - 1)
      if (Password == pass) {
        println("Login Succesfull")
        cmd = menu_main()
      } else {
        println("Password or username incorrect")
        Login(users)
      }
    }else{
      println("Password or username incorrect")
      Login(users)
    }

    cmd
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
        val fw = new FileWriter(f, true)
        try {
          fw.write(user+","+pass)
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
