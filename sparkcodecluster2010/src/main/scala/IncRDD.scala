package incrdd

import com.prat._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import reflect.ClassTag
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util._
import java.io.{IOException, ObjectOutputStream}


// Creating a custom RDD, IncRDD that extends RDD to provide mutability
// IncRDD has operations on <Key, value> pairs 

class IncRDD[K: ClassTag, V: ClassTag](
//IncRDD is an RDD of custom partitions
private val newRDD: RDD[IncrRDDPartition[K, V]]) 
extends RDD[(K, V)](newRDD.context,List(new OneToOneDependency(newRDD))) {

    //In custom RDD, override the default compute and getPartitions method
    //
    override protected def getPartitions: Array[Partition] = newRDD.partitions

    // computes the value of each partition
    override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
	firstParent[IncrRDDPartition[K, V]].iterator(split, context).next.iterator
    } 
/*
@throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = {
    // Update the reference to parent split at the time of task serialization
    var parentPartition = newRDD.partitions
    oos.defaultWriteObject()
}
*/
    override protected def getPreferredLocations(s: Partition): Seq[String] =
	newRDD.preferredLocations(s)

    override val partitioner = newRDD.partitioner

    // override default persist method
    override def persist(newLevel: StorageLevel): this.type = {
    	newRDD.persist(newLevel)
    	this
    }

    // Adds new <key,value> pairs to rdd
    // Returns a new version of IncRDD with the addition of elements
    def add(k: K, v: V): IncRDD[K, V] = 
	updateValue[V](Map(k -> v), (id, a) => a, (id, a, b) => b)

    // Modifies the existing value of a key
    // Returns a new version of IncRDD with the addition of elements 
    def update(k: K, v: V): IncRDD[K, V] = 
	updateValue[V](Map(k -> v), (id, a) => a, (id, a, b) => b)

    // Updates the value of a key K
    // Returns a new version of IncRDD with the modifications
    // executes the function 'func' if old and new values exists
    // Otherwise old func is executed.
    def updateValue[U: ClassTag](kvs: Map[K, U], oldfunc: (K, U) => V, func: (K, V, U) => V): IncRDD[K, V] = {
	val modifications = context.parallelize(kvs.toSeq).partitionBy(partitioner.get)
	val partitioned = modifications.partitionBy(partitioner.get)
	val newPartitionsRDD = newRDD.zipPartitions(partitioned, true)(new UpdateZipper(oldfunc,func))
	new IncRDD(newPartitionsRDD)	
    } 

    private type ZipPartitionFunc[V2, V3] =
    	Function2[Iterator[IncrRDDPartition[K, V]], Iterator[(K, V2)],Iterator[IncrRDDPartition[K, V3]]]


    private class UpdateZipper[U](oldfunc: (K, U) => V, func: (K, V, U) => V)  extends ZipPartitionFunc[U, V]
	with Serializable {
        def apply(thisIter: Iterator[IncrRDDPartition[K, V]], otherIter: Iterator[(K, U)])
            : Iterator[IncrRDDPartition[K, V]] = {
            val thisPart = thisIter.next()
            Iterator(thisPart.updateValue(otherIter, oldfunc, func))
    	}
    }

    // Deletes the specified key in the RDD
    // Returns a new version of IncRDD with deleted key, value
    // 
    def delete(ks: K): IncRDD[K, V] = {
    	val deletions = context.parallelize(Map(ks->()).toSeq).partitionBy(partitioner.get)
    	val partitioned = deletions.partitionBy(partitioner.get)
	val newPartitionsRDD = newRDD.zipPartitions(partitioned, true)(new RemoveZipper)
	new IncRDD(newPartitionsRDD)
    }

    private class RemoveZipper extends ZipPartitionFunc[Unit, V] with Serializable {
    	def apply(thisIter: Iterator[IncrRDDPartition[K, V]], otherIter: Iterator[(K, Unit)])
      	    : Iterator[IncrRDDPartition[K, V]] = {
      	    val thisPart = thisIter.next()
      	    Iterator(thisPart.delete(otherIter.map(_._1)))
    	}
    }

    // Fetch the value of a given key
    // 
    def get(key: K): IncRDD[K, V] = {
	val getting = context.parallelize(Map(key->()).toSeq).partitionBy(partitioner.get)
    	val partitioned = getting.partitionBy(partitioner.get)
	val newPartitionsRDD = newRDD.zipPartitions(partitioned, true)(new FetchZipper)
	new IncRDD(newPartitionsRDD)
    }
	
    private class FetchZipper extends ZipPartitionFunc[Unit, V] with Serializable {
    	def apply(thisIter: Iterator[IncrRDDPartition[K, V]], otherIter: Iterator[(K, Unit)])
      	: Iterator[IncrRDDPartition[K, V]] = {
      	val thisPart = thisIter.next()
      	Iterator(thisPart.get(otherIter.map(_._1)))
    	}
    }
}


object IncRDD {

    // constructs a mutable RDD 
    def apply[K: ClassTag , V: ClassTag](elems: RDD[(K, V)]): IncRDD[K, V] = 
	updatable[K, V, V](elems, (id, a) => a, (id, a, b) => b)

    // constructs the IncRDD from an RDD of partitions
    //
    def updatable[K: ClassTag,  U: ClassTag, V: ClassTag]
      (elements: RDD[(K, U)], oldfunc: (K, U) => V, func: (K, V, U) => V)
    : IncRDD[K, V] = {
        val partElements =
        if (elements.partitioner.isDefined) elements
        else elements.partitionBy(new HashPartitioner(elements.partitions.size))

        val newPartitions = partElements.mapPartitions[IncrRDDPartition[K, V]](
            iter => Iterator(IncrementalPartition(iter, oldfunc, func)),
            preservesPartitioning = true)
        new IncRDD(newPartitions)
    }
}

