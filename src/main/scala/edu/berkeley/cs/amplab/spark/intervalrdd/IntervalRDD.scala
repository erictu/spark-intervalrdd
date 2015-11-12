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
class IntervalRDD[S: ClassTag, V: ClassTag](
    /** The underlying representation of the IndexedRDD as an RDD of partitions. */
    private val partitionsRDD: RDD[IntervalPartition[S, V]])
  extends RDD[(V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) with Logging {

  //require(partitionsRDD.partitioner.isDefined)

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions 

  def getParts: Array[Partition] = getPartitions

  // // add bookkeeping private structure
  // // String is the chromosome/location, Long is partition number
  // var bookkeep: IntervalTree[String, Long] = new IntervalTree[String, Long]()

  /** Gets the value corresponding to the specified request, if any. 
  * a request contains the chromosome, interval, and specified keys
  */

  /** Provides the `RDD[(K, V)]` equivalent output. */
  override def compute(part: Partition, context: TaskContext): Iterator[V] = {
    // TODO
    null
  }

  def dumpPartitions() = {
    println("Partition Dump")
    partitionsRDD.foreachPartition(
      part => part.asInstanceOf[IntervalPartition[S, V]].getTree().printNodes
    )
  }

  def get(region: ReferenceRegion, k: S): Option[Map[ReferenceRegion, List[(S, V)]]] = multiget(region, Option(List(k)))

  // Gets values corresponding to a single interval over multiple ids
  def get(region: ReferenceRegion): Option[Map[ReferenceRegion, List[(S, V)]]] = multiget(region, None)

  /** Gets the values corresponding to the specified key, if any 
  * Assume that we're only getting data that exists (if it doesn't exist,
  * would have been handled by upper LazyMaterialization layer
  */
  def multiget(region: ReferenceRegion, ks: Option[List[S]]): Option[Map[ReferenceRegion, List[(S, V)]]] = IntervalTimers.MultigetTime.time { 

    val ksByPartition: Int = partitioner.get.getPartition(region)
    val partitions: Seq[Int] = Array(ksByPartition).toSeq

    val results: Array[Array[(ReferenceRegion, List[(S, V)])]] = IntervalTimers.ResultsTime.time {
      context.runJob(partitionsRDD, (context: TaskContext, partIter: Iterator[IntervalPartition[S, V]]) => { 
       if (partIter.hasNext && (ksByPartition == context.partitionId)) { 
          val intPart = partIter.next()
          val startTime = System.currentTimeMillis
          IntervalTimers.Match.time{
            ks match {
              case Some(_) => IntervalTimers.PartMG.time{intPart.multiget(Iterator((region, ks.get))).toArray}
              case None     => IntervalTimers.PartGA.time{intPart.getAll(Iterator(region)).toArray}
            }
          }
          val endTime = System.currentTimeMillis
          println("MATCH PART TIME IS")
          println(endTime - startTime)
          println()
          ks match {
            case Some(_) => IntervalTimers.PartMG.time{intPart.multiget(Iterator((region, ks.get))).toArray}
            case None     => IntervalTimers.PartGA.time{intPart.getAll(Iterator(region)).toArray}
          }
       } else {
          Array.empty
       }
      }, partitions, allowLocal = true)
    }
    Option(results.flatten.toMap)
  }


  /**
   * Unconditionally updates the specified keys to have the specified value. Returns a new IntervalRDD
   - outstanding issues: zipPartitions can only work for rdds of the same size
   - something odd is happening when you try to merge partitions together of different partition keys
   **/
  def multiput(elems: RDD[(ReferenceRegion, (S,V))], dict: SequenceDictionary): IntervalRDD[S, V] = {
    val partitioned = 
      if (elems.partitioner.isDefined) elems
      else {
        elems.partitionBy(new ReferencePartitioner(dict)) 
      }
    val convertedPartitions: RDD[IntervalPartition[S, V]] = partitioned.mapPartitions[IntervalPartition[S, V]]( 
      iter => Iterator(IntervalPartition(iter)), 
      preservesPartitioning = true)

    // merge the new partitions with existing partitions
    val merger = new PartitionMerger[S, V]()
    val newPartitionsRDD = partitionsRDD.zipPartitions(convertedPartitions, true)((aiter, biter) => merger(aiter, biter))
    new IntervalRDD(newPartitionsRDD)

  }

}

class PartitionMerger[S: ClassTag, V: ClassTag]() extends Serializable {
  def apply(thisIter: Iterator[IntervalPartition[S, V]], otherIter: Iterator[IntervalPartition[S, V]]): Iterator[IntervalPartition[S, V]] = { 
    var res: Map[ReferenceRegion, IntervalPartition[S, V]] = Map()

    while(thisIter.hasNext) {
      val thisPart = thisIter.next
      if (thisPart.getRegion.referenceName != "")
        res += (thisPart.getRegion -> thisPart)
    }

    while (otherIter.hasNext) {
      val otherPart = otherIter.next
      if (res.get(otherPart.getRegion) != None) {
        res += (otherPart.getRegion -> res.get(otherPart.getRegion).get.mergePartitions(otherPart) )
      } else {
        res += (otherPart.getRegion -> otherPart)
      }
    }
    res.iterator.map(a => a._2)
  }
}

object IntervalRDD extends Logging {
  /**
  * Constructs an updatable IntervalRDD from an RDD of a BDGFormat where partitioned by chromosome
  * TODO: Support different partitioners
  */
  def apply[S: ClassTag, V: ClassTag](elems: RDD[(ReferenceRegion, (S, V))], dict: SequenceDictionary) : IntervalRDD[S, V] = IntervalTimers.InitTime.time {
    val partitioned = 
      if (elems.partitioner.isDefined) elems
      else {
        elems.partitionBy(new ReferencePartitioner(dict)) 
      }
    val convertedPartitions: RDD[IntervalPartition[S, V]] = partitioned.mapPartitions[IntervalPartition[S, V]]( 
      iter => Iterator(IntervalPartition(iter)), 
      preservesPartitioning = true)

    new IntervalRDD(convertedPartitions) 
  }
}
