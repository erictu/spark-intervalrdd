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

import scala.collection.immutable.LongMap
import scala.reflect.ClassTag
import org.apache.spark.HashPartitioner


import com.github.akmorrow13.intervaltree._
import org.scalatest._
import org.apache.spark.{ SparkConf, Logging, SparkContext }
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.utils.instrumentation.Metrics
import org.bdgenomics.utils.instrumentation.{RecordedMetrics, MetricsListener}
import org.apache.spark.rdd.MetricsContext._

import org.scalatest.FunSuite
import org.scalatest.Matchers
import java.io.PrintWriter
import java.io.StringWriter
import java.io.OutputStreamWriter
import org.bdgenomics.adam.util.ADAMFunSuite

class IntervalRDDSuite extends ADAMFunSuite with Logging {

  val partitions = 100 
  // val metricsListener = new MetricsListener(new RecordedMetrics())
  // // sc.addSparkListener(metricsListener)
  // Metrics.initialize(sc)


  // sparkTest("create IntervalRDD from RDD using apply") {
  //   //creating intervals 
  //   // val metricsListener = new MetricsListener(new RecordedMetrics())
  //   // // sc.addSparkListener(metricsListener)
  //   // Metrics.initialize(sc)

  //   val int1: Interval[Long] = new Interval(0L, 99L)
  //   val int2: Interval[Long] = new Interval(100L, 199L)
  //   val int3: Interval[Long] = new Interval(200L, 299L)
  //   val k1 = ("chr1", int3)
  //   val k2 = ("chr2", int3)
  //   val k3 = ("chr3", int3)

  //   var intArr = Array(k1, k2, k3)
  //   var intArrRDD: RDD[(String, Interval[Long])] = sc.parallelize(intArr)

  //   //creating data
  //   val rec1: (String, String) = ("person1", "data for person 1, recordval1 0-99")
  //   val rec2: (String, String) = ("person2", "data for person 2, recordval2 100-199")
  //   val rec3: (String, String) = ("person3", "data for person 3, recordval3 200-299")
  //   var recArr: Array[(String, String)] = Array(rec1, rec2, rec3)
  //   var recArrRDD: RDD[(String, String)] = sc.parallelize(recArr)
  //   var zipped: RDD[((String, Interval[Long]), (String, String))] = intArrRDD.zip(recArrRDD)

  //   //initializing IntervalRDD with certain values
  //   var testRDD: IntervalRDD[String, Interval[Long], String, String] = IntervalRDD(zipped)
    
  //   // val writer = new PrintWriter(new OutputStreamWriter(System.out, "UTF-8"))
  //   // Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
  //   // writer.close()

  //   assert(1 == 1)

  // }

  // sparkTest("get one interval, k value") {
  //   val metricsListener = new MetricsListener(new RecordedMetrics())
  //   // sc.addSparkListener(metricsListener)
  //   Metrics.initialize(sc)

  //   val int1: Interval[Long] = new Interval(0L, 99L)
  //   val int2: Interval[Long] = new Interval(100L, 199L)
  //   val int3: Interval[Long] = new Interval(200L, 299L)
  //   val k1 = ("chr1", int2)
  //   val k2 = ("chr2", int2)
  //   val k3 = ("chr3", int3)

  //   var intArr = Array(k1, k2, k3)
  //   var intArrRDD: RDD[(String, Interval[Long])] = sc.parallelize(intArr, 3)

  //   //creating data
  //   val v1 = "data for person 1, recordval1 100-199"
  //   val v2 = "data for person 2, recordval2 100-199"
  //   val v3 = "data for person 3, recordval3 200-299"

  //   val rec1: (String, String) = ("person1", v1)
  //   val rec2: (String, String) = ("person2", v2)
  //   val rec3: (String, String) = ("person3", v3)

  //   var recArr: Array[(String, String)] = Array(rec1, rec2, rec3)
  //   var recArrRDD: RDD[(String, String)] = sc.parallelize(recArr, 3)
  //   var zipped: RDD[((String, Interval[Long]), (String, String))] = intArrRDD.zip(recArrRDD)

  //   //initializing IntervalRDD with certain values
  //   var testRDD: IntervalRDD[String, Interval[Long], String, String] = IntervalRDD(zipped)
    
  //   var mappedResults: Option[Map[Interval[Long], List[(String, String)]]] = testRDD.get("chr1", int2)
  //   var results = mappedResults.get
  //   assert(results.head._2.head._2 == v1)

  //   mappedResults = testRDD.get("chr3", int3)
  //   results = mappedResults.get
  //   val writer = new PrintWriter(new OutputStreamWriter(System.out, "UTF-8"))
  //   Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
  //   writer.close()

  //   assert(results.head._2.head._2 == v3)
  //   // assert(0==0)

  // }

  sparkTest("put multiple intervals into RDD to existing chromosome") {

    val metricsListener = new MetricsListener(new RecordedMetrics())
    sc.addSparkListener(metricsListener)
    Metrics.initialize(sc)

    val int1: Interval[Long] = new Interval(0L, 99L)
    val int2: Interval[Long] = new Interval(100L, 199L)
    val int3: Interval[Long] = new Interval(200L, 299L)
    val k1 = ("chr1", int1)
    val k2 = ("chr2", int2)
    val k3 = ("chr3", int3)

    var intArr = Array(k1, k2, k3)
    var intArrRDD: RDD[(String, Interval[Long])] = sc.parallelize(intArr)

    //creating data
    val v1 = "data for person 1, recordval1 0-99"
    val v2 = "data for person 2, recordval2 100-199"
    val v3 = "data for person 3, recordval3 200-299"

    val rec1: (String, String) = ("person1", v1)
    val rec2: (String, String) = ("person2", v2)
    val rec3: (String, String) = ("person3", v3)
    var recArr: Array[(String, String)] = Array(rec1, rec2, rec3)
    var recArrRDD: RDD[(String, String)] = sc.parallelize(recArr)
    var zipped: RDD[((String, Interval[Long]), (String, String))] = intArrRDD.zip(recArrRDD)

    //initializing IntervalRDD with certain values
    var testRDD: IntervalRDD[String, Interval[Long], String, String] = IntervalRDD(zipped)

    intArr = Array(("chr1", int2), ("chr2", int3))
    intArrRDD = sc.parallelize(intArr)

    val v4 = "data for person 1, recordval 100 - 199"
    val v5 = "data for person 2, recordval 200 - 299"

    val rec4: (String, String) = ("person1", v4)
    val rec5: (String, String) = ("person2", v5)

    recArr = Array(rec4, rec5)
    recArrRDD = sc.parallelize(recArr)
    zipped = intArrRDD.zip(recArrRDD)

    val newRDD: IntervalRDD[String, Interval[Long], String, String] = testRDD.multiput(zipped)

    var mappedResults: Option[Map[Interval[Long], List[(String, String)]]] = newRDD.get("chr2", int3)
    var results = mappedResults.get
    println(results)

    val writer = new PrintWriter(new OutputStreamWriter(System.out, "UTF-8"))
    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    writer.close()

    assert(results.head._2.head._2 == v5)
  }

}
