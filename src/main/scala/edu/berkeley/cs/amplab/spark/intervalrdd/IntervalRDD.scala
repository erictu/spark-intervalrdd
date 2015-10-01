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
  def get(chr: String, intl: Interval[Long], k: K): Option[V] = multiget(chr, intl, Array(k))

  // Gets values corresponding to a single interval over multiple ids
  def get(chr: String, intl: Interval[Long]): Option[V] = multiget(chr, intl, None)

  /** Gets the values corresponding to the specified key, if any 
  * Assume that we're only getting data that exists (if it doesn't exist,
  * would have been handled by upper LazyMaterialization layer
  */
  def multiget(chr: String, intl: Interval[Long], ks: Option[Array[K]]): Map[K, V] = {
    // check bookkeeping structure for partition number
    var partitionNum = bookkeep.search(intl, chr)

    var partition = partitionsRDD.getPartition(partitionNum) // TODO: Assume this exists
    // data exists in RDD, and we have the partition. Now let's fetch it
    var fetchedData = None
    ks match {
      case Some(ks)  => fetchedData = partition.get(intl, ks)
      case None      => fetchedData = partition.get(intl)
    }
    fetchedData
  }

  /**
   * Unconditionally updates the specified key to have the specified value. Returns a new IntervalRDD
   * that reflects the modification.
   */
  def put(chr: String, intl: Interval[Long], k: K, v: V): IntervalRDD[K, V] = multiput(chr, intl, Map(k -> v)) 

  /**
   * Unconditionally updates the specified keys to have the specified value. Returns a new IntervalRDD
   * that reflects the modification.
   */
  def multiput(chr: String, intl: Interval[Long], kvs: Map[K, V]): IntervalRDD[K, V] = {
    // update bookkeeping structure to insert entries of (chr, interval)
    // in the bookkeeping structure place the partition number
    // TODO: how to generate the partition number: use a hash function and put the bookkeep somewhere else?

    // then create an IntervalPartition with the subset of data provided, and the partition number, and add that to our RDD
    // by returning a newRDD

  }



}
