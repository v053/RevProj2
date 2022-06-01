import scala.util.control.Breaks._
import DataVisualizer._

//TODO: local imports for what's needed 
//For data visualization, 
//Generator?, (omitted atm)
//from exterrior query files (if functionality not includedd here) 

object Main { 
//-----------------------------------------------//
  def main(args:Array[String]): Unit = {

    var exit : Boolean = false;   //exit program flag  
    var cmd : String = " ";       //active input read 

    cmd = menu_main();            //inital menu print / choose option

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
  
}
