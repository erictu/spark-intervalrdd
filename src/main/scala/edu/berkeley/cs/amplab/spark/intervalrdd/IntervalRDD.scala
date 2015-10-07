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

// K = interval
// S = sec key
// V = data
class IntervalRDD[K: ClassTag, S: ClassTag, V: ClassTag](
    /** The underlying representation of the IndexedRDD as an RDD of partitions. */
    private val partitionsRDD: RDD[IntervalPartition[S, V]])
  extends RDD[(K, V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  require(partitionsRDD.partitioner.isDefined)

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions // TODO

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

    val ksByPartition: Int = partitioner.get.getPartition(chr)
    val partitions: Seq[Int] = Array(ksByPartition).toSeq

    val results: Array[Array[(Interval[Long], List[(S, V)])]] = context.runJob(partitionsRDD,
      (context: TaskContext, partIter: Iterator[IntervalPartition[S, V]]) => { 
       if (partIter.hasNext && (ksByPartition == context.partitionId)) { //if it's a partition that contains data we want
          val intPart = partIter.next()
          ks match {
            case Some(_) => intPart.multiget(Iterator((intl, ks.get))).toArray
            case None     => intPart.getAll(Iterator(intl)).toArray // TODO implement getting the data from iterators because these methods return iterators
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
  def multiput(chr: String, kvs: RDD[(K, (S,V))]): IntervalRDD[K, S, V] = {

    val newData: RDD[(K, (S,V))] = kvs.partitionBy(partitionsRDD.partitioner.get)

    // not sure if it goes to the correct partitions? Intervals should map to same partitions, but double check
    // theoretically, the data we have should all be one partition
    var partitionList: ListBuffer[Long] = new ListBuffer[Long]()

    //NOTE: partitions key by entityid, we only needed to key by interval at first to hash by interval
    val convertedPartitions: RDD[IntervalPartition[S,V]] = newData.mapPartitionsWithIndex( 
      (idx, iter) => {
        partitionList += idx //how to tell if partition exists? How is this going to add a new partition?
        Iterator(IntervalPartition(iter)) 
      }, preservesPartitioning = true)

    // we use the chr parameter to create our list of chr -> partition mappings
    var chrPartMap: List[(String, Long)] = partitionList.toList.map(id => (chr, id))
    val merger = new PartitionMerger[K,S,V]()
    val newPartitionsRDD = partitionsRDD.zipPartitions(convertedPartitions, true)((aiter, biter) => merger(aiter, biter))
   new IntervalRDD(newPartitionsRDD)
  }
}

class PartitionMerger[K: ClassTag, S: ClassTag, V: ClassTag]() {
  def apply(thisIter: Iterator[IntervalPartition[S, V]], otherIter: Iterator[IntervalPartition[S, V]]): Iterator[IntervalPartition[S, V]] = {
    // TODO: this is not right/needs to be more filtered
    // merge chr's to same partition

    // elif add partition
    val thisPart = thisIter.next()
    val otherPart = otherIter.next()


    Iterator(thisPart, otherPart)
  }
}

object IntervalRDD {
  /**
  * Constructs an updatable IntervalRDD from an RDD of a BDGFormat where partitioned by chromosome
  */
  // TODO: RDD should be of form RDD[(chr, (interval, (S,V)))]
  def apply[K: ClassTag, S: ClassTag, V: ClassTag](elems: RDD[(Interval[Long], (S, V))]) : IntervalRDD[K, S, V] = {
    val partitioned = 
      if (elems.partitioner.isDefined) elems
      else elems.partitionBy(new HashPartitioner(elems.partitions.size))
    val convertedPartitions: RDD[IntervalPartition[S,V]] = partitioned.mapPartitions( 
      iter => Iterator(IntervalPartition(iter)), 
      preservesPartitioning = true)
    new IntervalRDD(convertedPartitions) 
  }
}
