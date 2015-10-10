/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.amplab.spark.intervalrdd

import scala.reflect.ClassTag

import org.apache.spark.Dependency
import org.apache.spark.Partition
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

import com.github.akmorrow13.intervaltree._

// K = chr, interval
// S = sec key
// V = data
class IntervalRDD[C: ClassTag, K: ClassTag, S: ClassTag, V: ClassTag](
    /** The underlying representation of the IndexedRDD as an RDD of partitions. */
    private val partitionsRDD: RDD[IntervalPartition[C, S, V]])
  extends RDD[(K, V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  require(partitionsRDD.partitioner.isDefined)

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions 

  def getParts: Array[Partition] = getPartitions

  // // add bookkeeping private structure
  // // String is the chromosome/location, Long is partition number
  // var bookkeep: IntervalTree[String, Long] = new IntervalTree[String, Long]()

  /** Gets the value corresponding to the specified request, if any. 
  * a request contains the chromosome, interval, and specified keys
  */

  /** Provides the `RDD[(K, V)]` equivalent output. */
  override def compute(part: Partition, context: TaskContext): Iterator[(K, V)] = {
    // TODO
    null
  }

  // TODO replace Interval Type with K
  def get(chr: String, intl: Interval[Long], k: S): Option[Map[Interval[Long], List[(S, V)]]] = multiget(chr, intl, Option(List(k)))

  // Gets values corresponding to a single interval over multiple ids
  def get(chr: String, intl: Interval[Long]): Option[Map[Interval[Long], List[(S, V)]]] = multiget(chr, intl, None)

  /** Gets the values corresponding to the specified key, if any 
  * Assume that we're only getting data that exists (if it doesn't exist,
  * would have been handled by upper LazyMaterialization layer
  */
  // TODO: this is not multiget, can only pull from 1 partition
  def multiget(chr: String, intl: Interval[Long], ks: Option[List[S]]): Option[Map[Interval[Long], List[(S, V)]]] = { 

    val k = (chr, intl)
    val ksByPartition: Int = partitioner.get.getPartition(k)
    val partitions: Seq[Int] = Array(ksByPartition).toSeq

    val results: Array[Array[(Interval[Long], List[(S, V)])]] = context.runJob(partitionsRDD,
      (context: TaskContext, partIter: Iterator[IntervalPartition[C, S, V]]) => { 
       if (partIter.hasNext && (ksByPartition == context.partitionId)) { 
          val intPart = partIter.next()
          ks match {
            case Some(_) => intPart.multiget(Iterator((intl, ks.get))).toArray
            case None     => intPart.getAll(Iterator(intl)).toArray 
          }
       } else {
          Array.empty
       }
      }, partitions, allowLocal = true)
    Option(results.flatten.toMap)
  }

  /**
   * Unconditionally updates the specified key to have the specified value. Returns a new IntervalRDD
   * that reflects the modification.
   */
  // def put(chr: String, intl: Interval[Long], k: K, v: V): IntervalRDD[K, V] = multiput(chr, intl, Map(k -> v)) 

  /**
   * Unconditionally updates the specified keys to have the specified value. Returns a new IntervalRDD
   * that reflects the modification.
   * K is entityId, maps to V, which is the values
   * workflow should be the below: to be handled in upper layer?
   * var viewRegion = ReferenceRegion(0, 100)
   * var readsRDD: RDD[AlignmentRecord] = sc.loadAlignments("../workfiles/mouse_chrM.bam")
   * var idsRDD: RDD[Interval[Long] = readsRDD.map(new Interval(_.start, _.end))
   * var entityValRDD = readsRDD.map((_.personId, _))
   *    MAKE KEY THE INTERVAL SO HASHES INTO CORRECT PARTITION
   * var dataRDD: RDD[(Interval[Long], (Long, AlignmentRecord)] =  entityValRDD.zip(idsRDD)
   * intervalRDD.multiput("chrM", new Interval(viewRegion.start, viewRegion.end), dataRDD)
   */
  def multiput(kvs: RDD[((C,K), (S,V))]): IntervalRDD[C, K, S, V] = {

    val newData: RDD[((C, K), (S,V))] = kvs.partitionBy(partitionsRDD.partitioner.get)

    var partitionList: ListBuffer[Long] = new ListBuffer[Long]()

    // convert all partitions of the original RDD to Interval Partitions
    val convertedPartitions: RDD[IntervalPartition[C, S, V]] = newData.mapPartitionsWithIndex( 
      (idx, iter) => {
        partitionList += idx 
        Iterator(IntervalPartition(iter)) 
      }, preservesPartitioning = true)

    // merge the new partitions with existing partitions
    val merger = new PartitionMerger[C,K,S,V]()
    val newPartitionsRDD = partitionsRDD.zipPartitions(convertedPartitions, true)((aiter, biter) => merger(aiter, biter))

    new IntervalRDD(newPartitionsRDD)

  }

}

class PartitionMerger[C: ClassTag, K: ClassTag, S: ClassTag, V: ClassTag]() extends Serializable {
  def apply(thisIter: Iterator[IntervalPartition[C, S, V]], otherIter: Iterator[IntervalPartition[C, S, V]]): Iterator[IntervalPartition[C, S, V]] = {
    // TODO: dont merge so much!
    var res: ListBuffer[IntervalPartition[C, S, V]] = new ListBuffer[IntervalPartition[C, S, V]]()
    var otherPart: IntervalPartition[C, S, V] = null
    var thisPart: IntervalPartition[C, S, V] = null
    while(otherIter.hasNext) {
      otherPart = otherIter.next

      if (thisIter.hasNext) {
        thisPart = thisIter.next
      }

      if (otherPart.getId == thisPart.getId) {
        res += thisPart.mergePartitions(otherPart)
      } else {
        res += otherPart
      }
    }

    while(thisIter.hasNext) {
      res += thisIter.next()
    }
    res.iterator
  }
}

class ChrPartitioner[C, K](
    partitions: Int)
  extends Partitioner {

 private val hash = new HashPartitioner(partitions)

 def numPartitions: Int = partitions

  def getPartition(k: Any): Int = {
    // TODO: should add partitioning for interval, which is a component of k
    val tuple = k.asInstanceOf[(C, K)]
    hash.getPartition(tuple._1)
  }
}

object IntervalRDD {
  /**
  * Constructs an updatable IntervalRDD from an RDD of a BDGFormat where partitioned by chromosome
  */
  def apply[C: ClassTag, K: ClassTag, S: ClassTag, V: ClassTag](elems: RDD[((C, K), (S, V))]) : IntervalRDD[C, K, S, V] = {
    val partitioned = 
      if (elems.partitioner.isDefined) elems
      // TODO: remove hardcode for partition count
      else elems.partitionBy(new ChrPartitioner(10))
    val convertedPartitions: RDD[IntervalPartition[C, S, V]] = partitioned.mapPartitions( 
      iter => Iterator(IntervalPartition(iter)), 
      preservesPartitioning = true)
    new IntervalRDD(convertedPartitions) 
  }
}
