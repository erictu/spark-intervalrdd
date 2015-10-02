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


class IntervalRDD[K: ClassTag, V: ClassTag](
    /** The underlying representation of the IndexedRDD as an RDD of partitions. */
    private val partitionsRDD: RDD[IntervalPartition[K, V]])
  extends RDD[(K, V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {
  
  // add bookkeeping private structure
  // String is the chromosome/location, Long is partition number
  // TODO: make this more generalizable
  var bookkeep: IntervalTree[String, Long] = new IntervalTree[String, Long]()

  /** Gets the value corresponding to the specified request, if any. 
  * a request contains the chromosome, interval, and specified keys
  */
  def get(chr: String, intl: Interval[Long], k: K): Option[Map[K,V]] = multiget(chr, intl, Option(List(k)))

  // Gets values corresponding to a single interval over multiple ids
  def get(chr: String, intl: Interval[Long]): Option[Map[K,V]] = multiget(chr, intl, None)

  /** Gets the values corresponding to the specified key, if any 
  * Assume that we're only getting data that exists (if it doesn't exist,
  * would have been handled by upper LazyMaterialization layer
  */
  def multiget(chr: String, intl: Interval[Long], ks: Option[List[K]]): Option[Map[K, V]] = {
    // check bookkeeping structure for partition number
    var partitionNums: List[(String, Long)] = bookkeep.search(intl, chr)
    if (partitionNums.length == 0) {
      // return that it failed
      return None
    }
    val results: List[List[(K,V)]] = context.runJob(partitionsRDD,
      (context: TaskContext, partIter: Iterator[IntervalPartition[K, V]]) => {
        if (partIter.hasNext && partitionNums.contains(context.partitionId)) { //if it's a partition that contains data we want
          val intPart = partIter.next()
          ks match {
            case Some(ks) => intPart.multiget(Iterator((intl, ks)))
            case None     => intPart.multiget(Iterator((intl, ks))) // TODO
          }
        }
      }, partitions, allowLocal = true)
    Option(results.flatten.toMap)
  }

  /**
   * Unconditionally updates the specified key to have the specified value. Returns a new IntervalRDD
   * that reflects the modification.
   */
  def put(chr: String, intl: Interval[Long], k: K, v: V): IntervalRDD[K, V] = multiput(chr, intl, Map(k -> v)) 

  /**
   * Unconditionally updates the specified keys to have the specified value. Returns a new IntervalRDD
   * that reflects the modification.
   * K is entityId, maps to V, which is the values
   * TODO: should this be an RDD? How are we actually loading the data: do we need to do adamload and then convert to a map?
   * TODO: workflow should be the below: to be handled in upper layer?
   * var viewRegion = ReferenceRegion(0, 100)
   * var readsRDD_0to100: RDD[AlignmentRecord] = sc.loadAlignments("../workfiles/mouse_chrM.bam")
   * var idsRDD: RDD[Long] = RDD.map(_.personId)
   *    MAKE KEY THE INTERVAL SO HASHES INTO CORRECT PARTITION
   * var kvRDD: RDD[(Long, AlignmentRecord)] = readsRDD.zip(idsRDD)
   * var dataMap: Map[Long, AlignmentRecord] = kvRDD.collectAsMap()
   * intervalRDD.multiput("chrM", new Interval(viewRegion.start, viewRegion.end), dataMap)
   */
  def multiput(chr: String, intl: Interval[Long], kvs: Map[K, V]): IntervalRDD[K, V] = {
    //Do we need a new partitioner to hash by interval? what is it currently hashing by?
    val newData: RDD[(K,V)] = context.parallelize(kvs.toSeq).partitionBy(partitioner.get)
    // not sure if it goes to the correct partitions? Key should map to the same ones, but double check
    var partitionList: ListBuffer[Long] = new ListBuffer[Long]()
    val convertedPartitions: RDD[IntervalPartition[K,V]] = newData.mapPartitionsWithIndex(
      (idx, iter) => {
        partitionList += idx
        Iterator(IntervalPartition(iter))
      }, preservesPartitioning = true)
    //TODO: How to get K, the keys?
    // def insert(i: Interval[Long], r: List[(K, T)]): Boolean = {
    bookkeep.insert(intl, partitionList.toList)
    val newRDD: RDD[IntervalPartition[K,V]] = partitionsRDD.zipPartitions(convertedPartitions, true)
    new IntervalRDD(newRDD)
  }



}

object IntervalRDD {
  /**
  * Constructs an updatable IntervalRDD from an RDD of a BDGFormat
  */
  def apply[K: ClassTag, V: ClassTag](elems: RDD[(K,V)]) : IntervalRDD[K,V] = {
    //TODO: how to get K? It's just a tag
    val partitioned = 
      if (elems.partitioner.isDefined) elems
      else elems.partitionBy(new HashPartitioner(elems.partitions.size))
    val convertedPartitions: RDD[IntervalPartition[K,V]] = partitioned.mapPartitions(
      iter => Iterator(IntervalPartition(iter)) //need apply function for creating IntervalPartition from existing partition
      preservesPartitioning = true)
    new IntervalRDD(convertedPartitions) //sets partitionRDD
  }
}
