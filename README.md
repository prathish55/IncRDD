# IncRDD
#IncRDD is a new custom RDD, created to accept and process the incremental updates of the real-time data. 
We also provide a few APIs to the application programmer to make updates to RDD elements. This includes add, update, and delete.

Example:

import com.prat._
import org.apache.spark.rdd.RDD
import incrdd.IncRDD

object SimpleApp {
 def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    
    val rdd = sc.parallelize((1 to 100000).map(x => (x.toLong,0)))   
    val inrRDD= IncRDD(rdd).cache()                 //Incremental RDD

    var incup = inrRDD.add(10001L,55)
    for( x <- 100002L until 100002+l) { 
      incup=incup.add(x,55)
    }
    var incup = incup.update(10001L,55)
    var incup = incup.delete(1000L)
  }
}



Compile spark code :
./sbt/bin/sbt clean
./sbt/bin/sbt package

Execute :
spark-submit --spark-submit --master yarn-cluster --verbose --executor-memory 4G --executor-cores 7 --num-executors 6 --class Test.SimpleApp target/scala-2.10/simple-project_2.10-1.0.jar 1 0 10000

Arguments: <Choice save_to_file number_of_updates>
Choice : 1-insert, 2- delete , 3- update
save_to_file - 0- No, 1-Yes
number_of_updates - any number




