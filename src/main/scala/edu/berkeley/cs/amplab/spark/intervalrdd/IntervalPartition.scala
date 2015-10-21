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
import org.apache.spark.storage.StorageLevel

import edu.berkeley.cs.amplab.spark.intervalrdd._
import com.github.akmorrow13.intervaltree._
import org.apache.spark.Logging
import org.bdgenomics.adam.models.ReferenceRegion

// C = chr
// K = sec key
// V = data
// TODO: remove generics C
class IntervalPartition[K: ClassTag, V: ClassTag] 
	(val chr: String, val region: ReferenceRegion, protected val iTree: IntervalTree[K, V]) {

  def this(chr: String, region: ReferenceRegion) {
    this(chr, region, new IntervalTree[K, V]())
  }

  def getTree(): IntervalTree[K, V] = {
    iTree
  }

  def getId(): (String, ReferenceRegion) = {
    (chr, region)
  }

  protected def withMap
      (chr: String, region: ReferenceRegion, map: IntervalTree[K, V]): IntervalPartition[K, V] = {
    new IntervalPartition(chr, region, map)
  }


   // search by interval, return by (K=id, V=data)
  def getAll(ks: Iterator[ReferenceRegion]): Iterator[(ReferenceRegion, List[(K, V)])] = 
   ks.map { k => (k, iTree.search(k))  }

   // search by interval, return by (K=id, V=data)
  def multiget(ks: Iterator[(ReferenceRegion, List[K])]) : Iterator[(ReferenceRegion, List[(K, V)])] = 
   ks.map { k => (k._1, iTree.search(k._1, k._2))  }

  // k = interval
  // U = data: either a (S,V) tuple or List(S,V)
  def multiput(
      kvs: Iterator[(ReferenceRegion, List[(K, V)])]): IntervalPartition[K, V] = {
    val newTree = iTree.snapshot()
    for (ku <- kvs) {
      newTree.insert(ku._1, ku._2)
    }
    this.withMap(chr, region, newTree)
  }

  def mergePartitions(p: IntervalPartition[K, V]): IntervalPartition[K, V] = {
    val newTree = iTree.merge(p.getTree)
    this.withMap(chr, region, newTree)
  }

}

private[intervalrdd] object IntervalPartition {

  def apply[I: ClassTag, K: ClassTag, V: ClassTag]
      (iter: Iterator[((String, I), (K, V))]): IntervalPartition[K, V] = {
    val map = new IntervalTree[K, V]()
    // TODO: interval should change for partition granularity
    val region: ReferenceRegion = new ReferenceRegion("", 0L, 0L)

    // TODO: this blows!!!!
    var chr: String = "".asInstanceOf[String]
    iter.foreach { ku =>
      val chr: String = ku._1._1
      ku._1._2 match {
        // TODO: exception thrown if not Interval
        case n: ReferenceRegion => map.insert(ku._1._2.asInstanceOf[ReferenceRegion], ku._2)
        case _ => println("error") // TODO
      }
    }
    new IntervalPartition(chr, region, map)
  }
}