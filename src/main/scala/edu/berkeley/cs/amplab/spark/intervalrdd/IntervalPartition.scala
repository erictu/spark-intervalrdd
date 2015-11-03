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
	(val region: ReferenceRegion, protected val iTree: IntervalTree[K, V]) extends Serializable {

  def this(region: ReferenceRegion) {
    this(region, new IntervalTree[K, V]())
  }
  
  def this(iTree: IntervalTree[K, V]) = {
    this(new ReferenceRegion(iTree.root.region.referenceName, 0, 0), iTree)
  }

  def getTree(): IntervalTree[K, V] = {
    iTree
  }

  protected def withMap
      (map: IntervalTree[K, V]): IntervalPartition[K, V] = {
    new IntervalPartition(map)
  }

  def getRegion(): ReferenceRegion = {
    region
  }

   // search by interval, return by (K=id, V=data)
  def getAll(ks: Iterator[ReferenceRegion]): Iterator[(ReferenceRegion, List[(K, V)])] = {
    val startTime = System.currentTimeMillis
    val retVal = filterByRegion(ks.map { k => (k, iTree.search(k))  })
    val endTime = System.currentTimeMillis
    println("GETALL PART TIME IS")
    println(endTime - startTime)
    retVal
  }
  
   

   // search by interval, return by (K=id, V=data)
  def multiget(ks: Iterator[(ReferenceRegion, List[K])]) : Iterator[(ReferenceRegion, List[(K, V)])] = PartTimers.PartGetTime.time {
    val startTime = System.currentTimeMillis
    val retVal = filterByRegion(ks.map { k => (k._1, iTree.search(k._1, k._2))  })
    val endTime = System.currentTimeMillis
    println("MULTIGET PART TIME IS")
    println(endTime - startTime)
    retVal
  }


  def multiput(
      kvs: Iterator[(ReferenceRegion, List[(K, V)])]): IntervalPartition[K, V] = PartTimers.PartPutTime.time {
    val newTree = iTree.snapshot()
    for (ku <- kvs) {
      newTree.insert(ku._1, ku._2)
    }
    this.withMap(newTree)
  }

  def mergePartitions(p: IntervalPartition[K, V]): IntervalPartition[K, V] = {
    val newTree = iTree.merge(p.getTree)
    this.withMap(newTree)
  }

  private def filterByRegion(iter: Iterator[(ReferenceRegion, List[(K, V)])]): Iterator[(ReferenceRegion, List[(K, V)])] = PartTimers.FilterTime.time {
    val startTime = System.currentTimeMillis
    var newIter: ListBuffer[(ReferenceRegion, List[(K, V)])] = new ListBuffer[(ReferenceRegion, List[(K, V)])]()

    if (classOf[List[AlignmentRecord]].isAssignableFrom(classTag[V].runtimeClass)) {
      val aiter = iter.asInstanceOf[Iterator[(ReferenceRegion, List[(K, List[AlignmentRecord])])]]
      for (i <- aiter) {
        var data: ListBuffer[(K, List[AlignmentRecord])] = new ListBuffer()
        i._2.foreach(d => {
          data += ((d._1, d._2.filter(r => i._1.overlaps(new ReferenceRegion(i._1.referenceName, r.start, r.end)))))
        })
        newIter += ((i._1, data.asInstanceOf[ListBuffer[(K, V)]].toList))
      }
      val endTime = System.currentTimeMillis
      println("FILTER BY REGION TIMING IS")
      println(endTime - startTime)

      newIter.toIterator
    } else {
      println("Type not supported for filtering by Interval Partition. Data not filtered")
      
      val endTime = System.currentTimeMillis
      println("FILTER BY REGION TIMING IS")
      println(endTime - startTime)
      iter
    }
  }

}

private[intervalrdd] object IntervalPartition {

  def apply[K: ClassTag, V: ClassTag]
      (iter: Iterator[(ReferenceRegion, (K, V))]): IntervalPartition[K, V] = {
    val map = new IntervalTree[K, V]()
    // TODO: region should be set explicitly. This may cause issues
    var chr = ""
    iter.foreach { 
      ku => {
        if (chr == "")
          chr = ku._1.referenceName
        map.insert(ku._1, ku._2) 
      }
    }
    // TODO: 0, 0 can be mins and maxs to split a chr across partitions
    val partitionKey = new ReferenceRegion(chr, 0, 0)

    new IntervalPartition(partitionKey, map)
  }
}