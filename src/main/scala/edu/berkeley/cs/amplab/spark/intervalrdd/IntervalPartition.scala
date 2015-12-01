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
 * limitations under the License.f
 */

package edu.berkeley.cs.amplab.spark.intervalrdd

import scala.reflect.{classTag, ClassTag}

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer

import edu.berkeley.cs.amplab.spark.intervalrdd._
import com.github.akmorrow13.intervaltree._
import org.apache.spark.Logging
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ AlignmentRecord, Genotype }
import org.bdgenomics.utils.instrumentation.Metrics

object PartTimers extends Metrics {
  val PartGetTime = timer("Partition Multiget timer")
  val PartPutTime = timer("Partition Multiput timer")
  val FilterTime = timer("Filter timer")
}

class IntervalPartition[K: ClassTag, V: ClassTag]
	(protected val iTree: IntervalTree[K, V]) extends Serializable with Logging {

  def this() {
    this(new IntervalTree[K, V]())
  }

  def getTree(): IntervalTree[K, V] = {
    iTree
  }

  protected def withMap
      (map: IntervalTree[K, V]): IntervalPartition[K, V] = {
    new IntervalPartition(map)
  }

  /**
   * Gets all (k,v) data from partition within the specificed referenceregion
   *
   * @return Iterator of searched ReferenceRegion and the corresponding (K,V) pairs
   */
  def get(r: ReferenceRegion): Iterator[(K, V)] = {
    iTree.search(r)
  }

  /**
   * Gets data from partition within the specificed referenceregion and key k.
   *
   * @return Iterator of searched ReferenceRegion and the corresponding (K,V) pairs
   */
  def get(r: ReferenceRegion, k: K) : Iterator[(K, V)] = PartTimers.PartGetTime.time {
    multiget(r, List(k))
  }

  /**
   * Gets data from partition within the specificed referenceregion and key k.
   *
   * @return Iterator of searched ReferenceRegion and the corresponding (K,V) pairs
   */
  def multiget(r: ReferenceRegion, ks: List[K]) : Iterator[(K, V)] = PartTimers.PartGetTime.time {
    iTree.search(r, ks)
  }

  // TODO: remove code
  def setTime(): Double = {
    System.nanoTime()
  }
  def getTime(start: Double): Double = {
    val time = (System.nanoTime() - start)
    time/1e9
  }
  /**
   * Puts all (k,v) data from partition within the specificed referenceregion
   *
   * @return IntervalPartition with new data
   */
  def multiput(r: ReferenceRegion, kvs: Iterator[(K, V)]): IntervalPartition[K, V] = PartTimers.PartPutTime.time {
    val newTree = iTree.snapshot()
    newTree.insert(r, kvs)
    this.withMap(newTree)
  }

  /**
   * Merges trees of this partition with a specified partition
   *
   * @return Iterator of searched ReferenceRegion and the corresponding (K,V) pairs
   */
  def mergePartitions(p: IntervalPartition[K, V]): IntervalPartition[K, V] = {
    val newTree = iTree.merge(p.getTree)
    this.withMap(newTree)
  }
}

private[intervalrdd] object IntervalPartition {

  def apply[K: ClassTag, V: ClassTag]
      (iter: Iterator[(ReferenceRegion, (K, V))]): IntervalPartition[K, V] = {

    val map = new IntervalTree[K, V]()
    iter.foreach {
      ku => {
        map.insert(ku._1, ku._2)
      }
    }
    new IntervalPartition(map)
  }
}
