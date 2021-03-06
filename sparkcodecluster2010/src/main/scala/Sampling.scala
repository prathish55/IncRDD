package Test;

import java.io.File;
import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;

import incrdd.IncRDD
import com.sample
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

object Sampling {
	//var myRDD 
    def PrepareToSplit(l : String) : Tuple2[String,String] = {
               val parts = l.split(',')
               var yrMoDd = parts(0)+","+parts(1)+","+parts(2);
		// myRDD = myRDD.add(new Tuple2<String,String>(yrMoDd,l))
               return new Tuple2[String,String](yrMoDd,l);      
    }

    
    def main(args: Array[String]) {
        //System.out.println(System.getProperty("hadoop.home.dir"));
        
        val inputPath = "hdfs://localhost:9000/pxd141930/input/all.csv"
        val outputPath = "hdfs://localhost:9000/pxd141930_out/"
        val sampledAmt = args(0).toDouble
        
        /*Identify the original number of partitions*/
        val extensions = "bz2";        
       // var files = FileUtils.listFiles(new File(inputPath), extensions, false);
        val noOfPartitions = 10 // files.size();
        
        /*Delete output file. Do not do this in Production*/        
        FileUtils.deleteQuietly(new File(outputPath));
        
        /*Initialize Spark Context*/
        val conf = new SparkConf().setAppName("Sampling")

    	val sc = new SparkContext(conf)
        /*Read in the data*/
        val rdd = sc.textFile(inputPath);
        val data = sc.parallelize("a").map(x => ("a","b"))
	var inr = IncRDD(data).cache()
	var myRDD = inr.add("c","d")
        /*Process the data*/
	var h = IncRDD(rdd.sample(false, sampledAmt).map(l => PrepareToSplit(l)) )//.saveAsTextFile("hdfs://localhost:9000/pxd141930_s/")
	println("here")
	//for( x <- 1 until h.count()) { 
	h = h.update(h.first()._1,"hello")
	
	//}
	h.take(h.count().toInt).foreach( line => { myRDD = myRDD.add(line._1,line._2)
					h= h.update(line._1,"abc") } )
	h.saveAsTextFile("hdfs://localhost:9000/pxd141930_s/")	
                      
        myRDD.saveAsTextFile(outputPath); //Write to file

      
    }
}
