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
import org.bdgenomics.adam.models.{ Interval, ReferenceRegion }

class IntervalPartition[K <: Interval, V: ClassTag]
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
  def get(r: K): Iterator[V] = {
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

	def filter(r: K): IntervalPartition[K, V] = {
		val i: Iterator[V] = iTree.search(r)
    IntervalPartition(r, i)
  }

  /**
   * Return a new IntervalPartition filtered by some predicate
   */
  def filterGen(pred: V => Boolean): IntervalPartition[K, V] = {
    new IntervalPartition(iTree.treeFilt(pred))
  }

  /**
   * Puts all (k,v) data from partition within the specificed referenceregion
   *
   * @return IntervalPartition with new data
   */
  def multiput(r: K, vs: Iterator[V]): IntervalPartition[K, V] = {
    val newTree = iTree.snapshot()
    newTree.insert(r, vs)
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
  val chunkSize = 1000
  def matInterval[K <: Interval](region: K): K = {
    val start = region.start / chunkSize * chunkSize
    val end = region.end / chunkSize * chunkSize + (chunkSize - 1)

		// TODO: can you do this more generally?
		region match {
		case _: ReferenceRegion => new ReferenceRegion(region.asInstanceOf[ReferenceRegion].referenceName, start, end).asInstanceOf[K]
		case _ => {
				println("Type not supported for interval materialization")
				region
			}
		}
  }

  def apply[K <: Interval, V: ClassTag]
      (iter: Iterator[(K, V)]): IntervalPartition[K, V] = {
    val map = new IntervalTree[K, V]()
    iter.foreach {
      ku => {
        map.insert(matInterval(ku._1), ku._2)
      }
    }
    new IntervalPartition(map)
  }

  def apply[K <: Interval, V: ClassTag]
      (r: K, iter: Iterator[V]): IntervalPartition[K, V] = {
    val map = new IntervalTree[K, V]()
    map.insert(r, iter)
    new IntervalPartition(map)
  }

}
