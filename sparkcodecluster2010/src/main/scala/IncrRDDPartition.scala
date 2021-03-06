package incrdd

import com.prat._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import reflect.ClassTag
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import java.util.Map.Entry

//an abstract class for partition of RDD
//K <- Key
//V <- Value


abstract class IncrRDDPartition[K, V] extends Serializable{

    protected implicit def keyTag: ClassTag[K]
    protected implicit def valueTag: ClassTag[V]

    // This method is used to apply the hash and fetch the value
    //
    def apply(key: K): Option[V]

    def iterator: Iterator[(K, V)]

    def isDefined(key: K): Boolean =
	apply(key).isDefined

    // Updates the value of a key K, and executes the function 'func' if old and new values exists
    // in second hash map of partition
    // Returns new IncrRDDPartition version with new updates
    def updateValue[U](keyiter: Iterator[(K, U)], oldfunc: (K, U) => V, func: (K, V, U) => V): IncrRDDPartition[K, V] = 
	throw new UnsupportedOperationException("modifications not supported")

    // Deletes the specified key in the RDD
    //Returns new IncrRDDPartition version with deletions
    def delete(keyiter: Iterator[K]): IncrRDDPartition[K, V] =
	throw new UnsupportedOperationException("modifications not supported")

    // fetch the value of a given key in the partition
    //
    def get(keyiter: Iterator[K]): IncrRDDPartition[K, V] =
	throw new UnsupportedOperationException("modifications not supported")

}
