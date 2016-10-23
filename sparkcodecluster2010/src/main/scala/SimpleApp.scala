package Test
/* SimpleApp.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.prat._
import java.util.Properties
import java.util._
import org.apache.spark.rdd.RDD
import incrdd.IncRDD

object SimpleApp {


  def main(args: Array[String]) {
    //val logFile = "hdfs://localhost:9000/pxd141930/input/testfile.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")

    val sc = new SparkContext(conf)
    val rdd = sc.parallelize((1 to 100000).map(x => (x.toLong,0)))   
    val inrRDD= IncRDD(rdd).cache()
    val choice = args(0).toInt
    val save = args(1).toInt
    val l = args(2).toLong

    if (choice ==1) {
      var in3 = inrRDD.add(10001L,55)

      for( x <- 100002L until 100002+l) { 
        in3=in3.add(x,55)
      } 
      if (save ==1) {
        val in5= in3.saveAsTextFile("hdfs://localhost:9000/pxd141930_u/")
      }
    } else if (choice == 2) {

      var in3 = inrRDD.delete(100001L)
      for( x <- 100002L until 100002+l) { 
        in3=in3.delete(x)
      }
     if (save ==1) {
      val in5= in3.saveAsTextFile("hdfs://localhost:9000/pxd141930_u/")
     }
    } else if (choice == 3){	
      var in3 = inrRDD.update(10001L,55)
      println ("inside update")
      for( x <- 100002L-l until 100002) { 
        in3=in3.update(x,55)
      }
      if (save ==1) {
      val in5= in3.saveAsTextFile("hdfs://localhost:9000/pxd141930_u/")
    }
  }
  //val in4=in3.get(1L).saveAsTextFile("hdfs://localhost:9000/pxd141930_g/")
  //println("100001 value is %d".format(in4))
  }
}

