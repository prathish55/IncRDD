package incrdd

import com.prat._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import reflect.ClassTag
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import java.util.Map.Entry
import scala.collection.JavaConversions._
import org.apache.spark.Logging
import java.util.ArrayList

//Incremental partition 

private[incrdd] class IncrementalPartition[K, V]
(protected val cmap: CuckooHashMap[K, V])
(override implicit val keyTag: ClassTag[K],
override implicit val valueTag: ClassTag[V])
extends IncrRDDPartition[K, V]  {

    // We implement a new variant Dual-CHFast-Simple hashing technique
    // It has two cuckoo hash tables and uses multiplicative hash code
    // 'cmap' is the first cuckoo hash,
    // used to store <K,V> pairs in the partition

    // 'valuesmap' is second cuckoo hash, 
    // used to store old versions of objects - <K,V> pairs in <K,Arraylist<V>>
    //
    var valuesmap = new CuckooHashMap[K, ArrayList[V]]

    override def apply(k: K): Option[V] = {
	Option(cmap.get(k).asInstanceOf[V])
    }

    override def iterator: Iterator[(K, V)] = {
	cmap.iterator
    }

    override def updateValue[U](keyiter: Iterator[(K, U)], oldfunc: (K, U) => V, func: (K, V, U) => V): IncrRDDPartition[K, V] = {
	//For every key, update the value
	for( ku <- keyiter) {
	    val key = ku._1

	    // get the previous value for the key
	    // perform lookup to cuckoo hash for the key
	    //
	    val oldValue = cmap.get(ku._1).asInstanceOf[V]

	    // get the list of previous values for the key
            // valuesmap stores old versions
            //
	    var arraylist= valuesmap.get(ku._1)

	    if(arraylist == null) {
		arraylist= new ArrayList[V]	
	    } 
	    // update the new values to cuckoo hash map
	    arraylist.add(oldValue)
	    valuesmap.put(ku._1,arraylist)
	    val newValue= if(oldValue == null) oldfunc(ku._1,ku._2) else func(ku._1,oldValue,ku._2)
	    //  finally store the updated value
	    //
	    cmap.put(key,newValue)
	}
	new IncrementalPartition(cmap)
    }

    override def delete(keyiter: Iterator[K]): IncrRDDPartition[K, V] = {
	// delete for key
	//
    	for (key <- keyiter) {
		// Since the key is deleted with value,
		// Remove from both the cuckoo hash maps
		//	
	      	cmap.remove(key)
		valuesmap.remove(key)
    	}
    	new IncrementalPartition(cmap)
    }

    override def get(keyiter: Iterator[K]):  IncrRDDPartition[K, V]  =  {
	//Fetch the values in a new map
	//
	val getmap = new CuckooHashMap[K, V]
	getmap.put(keyiter.asInstanceOf[K], cmap.get(keyiter).asInstanceOf[V])
	new IncrementalPartition(getmap) 
    }	
}


private[incrdd] object IncrementalPartition{

    def apply[K: ClassTag, V: ClassTag]
      (iter: Iterator[(K, V)]) =
    apply[K, V, V](iter, (id, a) => a, (id, a, b) => b)

    def apply[K: ClassTag, U: ClassTag, V: ClassTag]
      (iter: Iterator[(K, U)], oldfunc: (K, U) => V, func: (K, V, U) => V): IncrementalPartition[K, V] = {
        val cmap = new CuckooHashMap[K, V]
        iter.foreach { ku =>
        	val key = ku._1
		// get the previous value for the key
	    	// perform lookup to cuckoo hash for the key
	    	//
        	val oldValue = cmap.get(key).asInstanceOf[V]
        	val newValue = if (oldValue == null) oldfunc(ku._1, ku._2) else func(ku._1, oldValue, ku._2)
        	cmap.put(key, newValue)
        }
        new IncrementalPartition(cmap)
    }
}
