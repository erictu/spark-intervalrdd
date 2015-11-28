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
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.instrumentation.Metrics

object PartTimers extends Metrics {
  val PartGetTime = timer("Partition Multiget timer")
  val PartPutTime = timer("Partition Multiput timer")
  val FilterTime = timer("Filter timer")
}
// K = sec key
// V = generic data blob
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
  def getAll(ks: Iterator[ReferenceRegion]): Iterator[(ReferenceRegion, List[(K, V)])] = {
    var input = ks.map { k => (k, iTree.search(k))  }
    filterByRegion(input)
  }
  
  /**
   * Gets data from partition within the specificed referenceregion and key k.
   *
   * @return Iterator of searched ReferenceRegion and the corresponding (K,V) pairs
   */
  def multiget(ks: Iterator[(ReferenceRegion, List[K])]) : Iterator[(ReferenceRegion, List[(K, V)])] = PartTimers.PartGetTime.time {
    println("IPMULTIGET")
    var input = ks.map { k => (k._1, iTree.search(k._1, k._2))  }
    filterByRegion(input)    
  }

  /**
   * Puts all (k,v) data from partition within the specificed referenceregion
   *
   * @return IntervalPartition with new data 
   */
  def multiput(
      kvs: Iterator[(ReferenceRegion, List[(K, V)])]): IntervalPartition[K, V] = PartTimers.PartPutTime.time {
    println("IPMULTIPUT: START")
    val newTree = iTree.snapshot()
    for (ku <- kvs) {
      println(ku._1)
      newTree.insert(ku._1, ku._2)
    }
    println("IPMULTIPUT: DONE")
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

  /**
   * Filters data pulled from partition by specific reference region to be searched over TODO: This is messy
   *
   * @return Filtered data within given referenceregion
   */
  private def filterByRegion(iter: Iterator[(ReferenceRegion, List[(K, V)])]): Iterator[(ReferenceRegion, List[(K, V)])] = PartTimers.FilterTime.time {
    var newIter: ListBuffer[(ReferenceRegion, List[(K, V)])] = new ListBuffer[(ReferenceRegion, List[(K, V)])]()
    println("IPFILTERBYREGION")
    // filters
    if (classOf[List[AlignmentRecord]].isAssignableFrom(classTag[V].runtimeClass)) {
      val aiter = iter.asInstanceOf[Iterator[(ReferenceRegion, List[(K, List[AlignmentRecord])])]]
      for (i <- aiter) {
        // go through all records and fliter alginmentrecods not in referenceregion
        var data: ListBuffer[(K, List[AlignmentRecord])] = new ListBuffer()
        i._2.foreach(d => {
          data += ((d._1, d._2.filter(r => i._1.overlaps(new ReferenceRegion(i._1.referenceName, r.start, r.end)))))
        })
        newIter += ((i._1, data.asInstanceOf[ListBuffer[(K, V)]].toList))
      }
      newIter.toIterator
    } else {
      log.warn("Type not supported for filtering by Interval Partition. Data not filtered")
      iter
    }
  }

}

private[intervalrdd] object IntervalPartition {

  def apply[K: ClassTag, V: ClassTag]
      (iter: Iterator[(ReferenceRegion, (K, V))]): IntervalPartition[K, V] = {
    println("IPAPPLY: START")
    val map = new IntervalTree[K, V]()
    iter.foreach { 
      ku => {
        println(ku._1)
        map.insert(ku._1, ku._2) 
      }
    }
    println("IPAPPLY: DONE")
    new IntervalPartition(map)
  }
}