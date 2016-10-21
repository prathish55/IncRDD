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
/*    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

   val cmap= new CuckooHashMap[Integer,ArrayList[Integer]]
   val obj1 = new CuckooObj(cmap)
   obj1.changeValue(1,102)
   obj1.changeValue(1,103)
   val obj2 =  new CuckooObj(cmap)
   obj2.changeValue(1,104)
   obj1.changeValue(1,105)
   println("obj1 val is %d".format(obj1.getValue(1)))
   println("obj2 val is %d".format(obj2.getValue(1)))
*/
  val rdd = sc.parallelize((1 to 100000).map(x => (x.toLong,0)))   
  val inrRDD= IncRDD(rdd).cache()
  //val in=inrRDD.saveAsTextFile("hdfs://localhost:9000/pxd141930_t/")
  //val in2= rdd.saveAsTextFile("hdfs://localhost:9000/pxd141930_s/")

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

