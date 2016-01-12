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

// TODO: is chunking on region required?

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

class IntervalPartition[V: ClassTag]
	(protected val iTree: IntervalTree[V]) extends Serializable with Logging {

  def this() {
    this(new IntervalTree[V]())
  }

  def getTree(): IntervalTree[V] = {
    iTree
  }

  protected def withMap
      (map: IntervalTree[V]): IntervalPartition[V] = {
    new IntervalPartition(map)
  }

  /**
   * Gets all (k,v) data from partition within the specificed referenceregion
   *
   * @return Iterator of searched ReferenceRegion and the corresponding (K,V) pairs
   */
  def get(r: ReferenceRegion): Iterator[V] = {
    iTree.search(r)
  }

  /**
   * Gets all (k,v) data from partition
   *
   * @return Iterator of searched ReferenceRegion and the corresponding (K,V) pairs
   */
  def get(): Iterator[V] = {
    iTree.get.toIterator
  }

  // TODO: test
  def filter(r: ReferenceRegion): IntervalPartition[V] = {
    val i: Iterator[V] = iTree.search(r)
    IntervalPartition(r, i)
  }

  /**
   * Puts all (k,v) data from partition within the specificed referenceregion
   *
   * @return IntervalPartition with new data
   */
  def multiput(r: ReferenceRegion, vs: Iterator[V]): IntervalPartition[V] = PartTimers.PartPutTime.time {
    val newTree = iTree.snapshot()
    newTree.insert(r, vs)
    this.withMap(newTree)
  }

  /**
   * Merges trees of this partition with a specified partition
   *
   * @return Iterator of searched ReferenceRegion and the corresponding (K,V) pairs
   */
  def mergePartitions(p: IntervalPartition[V]): IntervalPartition[V] = {
    val newTree = iTree.merge(p.getTree)
    this.withMap(newTree)
  }
}



private[intervalrdd] object IntervalPartition {
  val chunkSize = 1000
  def matRegion(region: ReferenceRegion): ReferenceRegion = {
    val start = region.start / chunkSize * chunkSize
    val end = region.end / chunkSize * chunkSize + (chunkSize - 1)
    new ReferenceRegion(region.referenceName, start, end)
  }

  def apply[V: ClassTag]
      (iter: Iterator[(ReferenceRegion, V)]): IntervalPartition[V] = {
    val map = new IntervalTree[V]()
    iter.foreach {
      ku => {
        map.insert(matRegion(ku._1), ku._2)
      }
    }
    new IntervalPartition(map)
  }

  def apply[V: ClassTag]
      (r: ReferenceRegion, iter: Iterator[V]): IntervalPartition[V] = {
    val map = new IntervalTree[V]()
    map.insert(r, iter)
    }
    new IntervalPartition(map)
  }
}
