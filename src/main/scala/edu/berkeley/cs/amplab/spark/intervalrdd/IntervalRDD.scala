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
import org.apache.spark.{ SparkConf, Logging, SparkContext }
import org.apache.spark.TaskContext
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ReferencePartitioner
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.utils.instrumentation.Metrics

import scala.collection.mutable.ListBuffer

import com.github.akmorrow13.intervaltree._


object IntervalTimers extends Metrics {
  val MultigetTime = timer("Multiget timer")
  val MultiputTime = timer("Multiput timer")
  val InitTime = timer("Initialize RDD")
  val ResultsTime = timer("Results Job")
  val PartMG = timer("Part Multiget")
  val PartGA = timer("Part Getall")
  val Match = timer("KS Match")
}

class IntervalRDD[V: ClassTag](
    /** The underlying representation of the IndexedRDD as an RDD of partitions. */
    private val partitionsRDD: RDD[IntervalPartition[V]])
  extends RDD[(V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) with Logging {

  require(partitionsRDD.partitioner.isDefined)
  partitionsRDD.persist()

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /** TODO: Provides the `RDD[(K, V)]` equivalent output. */
  override def compute(part: Partition, context: TaskContext): Iterator[V] = {
    null

  }

  /** Persists the edge partitions using `targetStorageLevel`, which defaults to MEMORY_ONLY. */
  override def cache(): this.type = {
    partitionsRDD.persist()
    this
  }

  /** The number of edges in the RDD. */
  override def count(): Long = {
    partitionsRDD.map(_.getTree.size).reduce(_ + _)
  }

  override def collect(): Array[V] = partitionsRDD.flatMap(r => r.get()).collect()

  def filterByRegion(r: ReferenceRegion): IntervalRDD[V] = {
    mapIntervalPartitions(r, (part) => part.filter(r))
  }

  def mapIntervalPartitions(r: ReferenceRegion,
      f: (IntervalPartition[V]) => IntervalPartition[V]): IntervalRDD[V] = {
    this.withPartitionsRDD[V](partitionsRDD.mapPartitions({ iter =>
      if (iter.hasNext) {
        val p = iter.next()
        Iterator(p.filter(r))
      } else {
        Iterator.empty
      }
    }, preservesPartitioning = true))
  }

  private def withPartitionsRDD[V2: ClassTag](
      partitionsRDD: RDD[IntervalPartition[V2]]): IntervalRDD[V2] = {
    new IntervalRDD(partitionsRDD)
  }

  /** Gets the values corresponding to the specified key, if any
  * Assume that we're only getting data that exists (if it doesn't exist,
  * would have been handled by upper LazyMaterialization layer
  */
  def get(region: ReferenceRegion): List[V] = IntervalTimers.MultigetTime.time {
    val ksByPartition: Int = partitioner.get.getPartition(region)
    val partitions: Seq[Int] = Array(ksByPartition).toSeq

    val results: Array[Array[V]] = IntervalTimers.ResultsTime.time {
      context.runJob(partitionsRDD, (context: TaskContext, partIter: Iterator[IntervalPartition[V]]) => {
       if (partIter.hasNext && (ksByPartition == context.partitionId)) {
          val intPart = partIter.next()
          intPart.get(region).toArray
       } else {
          Array.empty
       }
      }, partitions, allowLocal = true)
    }
    results.flatten.toList
  }

  /**
   * Unconditionally updates the specified keys to have the specified value. Returns a new IntervalRDD
   **/
  def multiput(elems: RDD[(ReferenceRegion, V)], dict: SequenceDictionary): IntervalRDD[V] = {
    val partitioned =
      if (elems.partitioner.isDefined) elems
      else {
        elems.partitionBy(new ReferencePartitioner(dict))
      }

    val convertedPartitions: RDD[IntervalPartition[V]] = partitioned.mapPartitions[IntervalPartition[V]](
      iter => Iterator(IntervalPartition(iter)),
      preservesPartitioning = true)

    // merge the new partitions with existing partitions
    val merger = new PartitionMerger[V]()
    val newPartitionsRDD = partitionsRDD.zipPartitions(convertedPartitions, true)((aiter, biter) => merger(aiter, biter))
    new IntervalRDD(newPartitionsRDD)
  }
}

class PartitionMerger[V: ClassTag]() extends Serializable {
  def apply(thisIter: Iterator[IntervalPartition[V]], otherIter: Iterator[IntervalPartition[V]]): Iterator[IntervalPartition[V]] = {
    val thisPart = thisIter.next
    val otherPart = otherIter.next
    Iterator(thisPart.mergePartitions(otherPart))
  }
}

object IntervalRDD extends Logging {
  /**
  * Constructs an updatable IntervalRDD from an RDD of a BDGFormat where partitioned by chromosome
  * TODO: Support different partitioners
  */
  def apply[V: ClassTag](elems: RDD[(ReferenceRegion, V)], dict: SequenceDictionary) : IntervalRDD[V] = IntervalTimers.InitTime.time {
    val partitioned =
      if (elems.partitioner.isDefined) elems
      else {
        elems.partitionBy(new ReferencePartitioner(dict))
      }
    val convertedPartitions: RDD[IntervalPartition[V]] = partitioned.mapPartitions[IntervalPartition[V]](
      iter => Iterator(IntervalPartition(iter)),
      preservesPartitioning = true)

    new IntervalRDD(convertedPartitions)
  }

  def apply[V: ClassTag](elems: RDD[IntervalPartition[V]]) : IntervalRDD[V] = {
    new IntervalRDD(elems)
  }

}
