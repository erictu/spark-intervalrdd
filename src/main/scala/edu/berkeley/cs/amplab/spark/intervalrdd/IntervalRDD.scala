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

// K = chr, interval
// S = sec key
// V = data
class IntervalRDD[K: ClassTag, V: ClassTag](
    /** The underlying representation of the IndexedRDD as an RDD of partitions. */
    private val partitionsRDD: RDD[IntervalPartition[K, V]])
  extends RDD[(V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) with Logging {

  require(partitionsRDD.partitioner.isDefined)

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /** TODO: Provides the `RDD[(K, V)]` equivalent output. */
  override def compute(part: Partition, context: TaskContext): Iterator[V] = {
    null
  }

  def get(region: ReferenceRegion, k: K): Map[K, V] = multiget(region, Option(List(k)))

  // Gets values corresponding to a single interval over multiple ids
  def get(region: ReferenceRegion): Map[K, V] = multiget(region, None)

  /** Gets the values corresponding to the specified key, if any
  * Assume that we're only getting data that exists (if it doesn't exist,
  * would have been handled by upper LazyMaterialization layer
  */
  def multiget(region: ReferenceRegion, ks: Option[List[K]]): Map[K, V] = IntervalTimers.MultigetTime.time {
    val ksByPartition: Int = partitioner.get.getPartition(region)
    val partitions: Seq[Int] = Array(ksByPartition).toSeq

    val results: Array[Array[(K, V)]] = IntervalTimers.ResultsTime.time {
      context.runJob(partitionsRDD, (context: TaskContext, partIter: Iterator[IntervalPartition[K, V]]) => {
       if (partIter.hasNext && (ksByPartition == context.partitionId)) {
          val intPart = partIter.next()
          ks match {
            case Some(_) =>  IntervalTimers.PartMG.time{intPart.multiget(region, ks.get).toArray}
            case None     => IntervalTimers.PartGA.time{intPart.get(region).toArray}
          }
       } else {
          Array.empty
       }
      }, partitions, allowLocal = true)
    }
    results.flatten.toMap
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
   * Unconditionally updates the specified keys to have the specified value. Returns a new IntervalRDD
   - outstanding issues: zipPartitions can only work for rdds of the same size
   - something odd is happening when you try to merge partitions together of different partition keys
   **/
  def multiput(elems: RDD[(ReferenceRegion, (K,V))], dict: SequenceDictionary): IntervalRDD[K, V] = {
    val partitioned =
      if (elems.partitioner.isDefined) elems
      else {
        elems.partitionBy(new ReferencePartitioner(dict))
      }

    val convertedPartitions: RDD[IntervalPartition[K, V]] = partitioned.mapPartitions[IntervalPartition[K, V]](
      iter => Iterator(IntervalPartition(iter)),
      preservesPartitioning = true)

    // merge the new partitions with existing partitions
    val merger = new PartitionMerger[K, V]()
    val newPartitionsRDD = partitionsRDD.zipPartitions(convertedPartitions, true)((aiter, biter) => merger(aiter, biter))
    new IntervalRDD(newPartitionsRDD)
  }
}

class PartitionMerger[K: ClassTag, V: ClassTag]() extends Serializable {
  def apply(thisIter: Iterator[IntervalPartition[K, V]], otherIter: Iterator[IntervalPartition[K, V]]): Iterator[IntervalPartition[K, V]] = {
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
  def apply[K: ClassTag, V: ClassTag](elems: RDD[(ReferenceRegion, (K, V))], dict: SequenceDictionary) : IntervalRDD[K, V] = IntervalTimers.InitTime.time {
    val partitioned =
      if (elems.partitioner.isDefined) elems
      else {
        elems.partitionBy(new ReferencePartitioner(dict))
      }
    val convertedPartitions: RDD[IntervalPartition[K, V]] = partitioned.mapPartitions[IntervalPartition[K, V]](
      iter => Iterator(IntervalPartition(iter)),
      preservesPartitioning = true)

    new IntervalRDD(convertedPartitions)
  }
}
