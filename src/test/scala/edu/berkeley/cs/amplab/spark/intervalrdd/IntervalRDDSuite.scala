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
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceRecord, SequenceDictionary }
import org.bdgenomics.utils.instrumentation.Metrics
import org.bdgenomics.utils.instrumentation.{RecordedMetrics, MetricsListener}
import org.apache.spark.rdd.MetricsContext._
import java.io.PrintWriter
import java.io.StringWriter
import java.io.OutputStreamWriter
import org.bdgenomics.adam.util.ADAMFunSuite
class IntervalRDDSuite extends ADAMFunSuite with Logging {

  val partitions = 100 

  sparkTest("create IntervalRDD from RDD using apply") {
    val metricsListener = new MetricsListener(new RecordedMetrics())
    sc.addSparkListener(metricsListener) //Uncommenting this makes sc shutdown
    Metrics.initialize(sc)

    val chr1 = "chr1"
    val chr2 = "chr2"
    val chr3 = "chr3"
    val region1: ReferenceRegion = new ReferenceRegion(chr1, 0L, 99L)
    val region2: ReferenceRegion = new ReferenceRegion(chr2, 100L, 199L)
    val region3: ReferenceRegion = new ReferenceRegion(chr3, 200L, 299L)

    //creating data
    val rec1: (String, String) = ("person1", "data for person 1, recordval1 0-99")
    val rec2: (String, String) = ("person2", "data for person 2, recordval2 100-199")
    val rec3: (String, String) = ("person3", "data for person 3, recordval3 200-299")

    var intArr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    var intArrRDD: RDD[(ReferenceRegion, (String, String))] = sc.parallelize(intArr)

    //initializing IntervalRDD with certain values
    //TODO: To get sequence dictionary, we need to use ADAMContext.adamDictionaryLoad(filePath)
      //So for now, let's just create one out of scratch
    //TODO: how to add to the sequence dictionary after making it?
      //Use SequenceDictionary.+ and SequenceDictionary.++, reset it to remake the partitioner each time
      //By each time, we mean each time we load in a new file. Should we support this or just assume 
      //We specify everything at the start of running?
    // val sd = new SequenceDictionary()
    //TODO: eventually we need not just alignment

    val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
      SequenceRecord("chr2", 1000L), 
      SequenceRecord("chr3", 1000L))) //NOTE: the number is the length of the chromosome

    var testRDD: IntervalRDD[String, String] = IntervalRDD(intArrRDD, sd)

    val stringWriter = new StringWriter()
    val writer = new PrintWriter(stringWriter)
    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    writer.flush()
    val timings = stringWriter.getBuffer.toString
    println(timings)
    logInfo(timings)

    assert(1 == 1)

  }

  sparkTest("get one interval, k value") {
    val metricsListener = new MetricsListener(new RecordedMetrics())
    sc.addSparkListener(metricsListener)
    Metrics.initialize(sc)

    val chr1 = "chr1"
    val chr2 = "chr2"
    val chr3 = "chr3"
    val region1: ReferenceRegion = new ReferenceRegion(chr1, 0L, 99L)
    val region2: ReferenceRegion = new ReferenceRegion(chr2, 100L, 199L)
    val region3: ReferenceRegion = new ReferenceRegion(chr3, 200L, 299L)

    //creating data
    val rec1: (String, String) = ("person1", "data for person 1, recordval1 0-99")
    val rec2: (String, String) = ("person2", "data for person 2, recordval2 100-199")
    val rec3: (String, String) = ("person3", "data for person 3, recordval3 200-299")

    var intArr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    var intArrRDD: RDD[(ReferenceRegion, (String, String))] = sc.parallelize(intArr)

    //See TODO flags above
    //initializing IntervalRDD with certain values
    val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
      SequenceRecord("chr2", 1000L),
      SequenceRecord("chr3", 1000L)))

    var testRDD: IntervalRDD[String, String] = IntervalRDD(intArrRDD, sd)
    
    var mappedResults: Option[Map[ReferenceRegion, List[(String, String)]]] = testRDD.get(region1)
    var results = mappedResults.get
    assert(results.head._2.head._2 == rec1._2)

    mappedResults = testRDD.get(region3)
    results = mappedResults.get

    val stringWriter = new StringWriter()
    val writer = new PrintWriter(stringWriter)
    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    writer.flush()
    val timings = stringWriter.getBuffer.toString
    println(timings)
    logInfo(timings)

    assert(results.head._2.head._2 == rec3._2)

  }

  sparkTest("put multiple intervals into RDD to existing chromosome") {
    val metricsListener = new MetricsListener(new RecordedMetrics())
    sc.addSparkListener(metricsListener)
    Metrics.initialize(sc)

    val chr1 = "chr1"
    val chr2 = "chr2"
    val chr3 = "chr3"
    val region1: ReferenceRegion = new ReferenceRegion(chr1, 0L, 99L)
    val region2: ReferenceRegion = new ReferenceRegion(chr2, 100L, 199L)
    val region3: ReferenceRegion = new ReferenceRegion(chr3, 200L, 299L)

    //creating data
    val rec1: (String, String) = ("person1", "data for person 1, recordval1 0-99")
    val rec2: (String, String) = ("person2", "data for person 2, recordval2 100-199")
    val rec3: (String, String) = ("person3", "data for person 3, recordval3 200-299")

    var intArr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    var intArrRDD: RDD[(ReferenceRegion, (String, String))] = sc.parallelize(intArr)

    //See TODO flags above
    //initializing IntervalRDD with certain values
    val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
      SequenceRecord("chr2", 1000L),
      SequenceRecord("chr3", 1000L)))

    var testRDD: IntervalRDD[String, String] = IntervalRDD(intArrRDD, sd)


    val v4 = "data for person 1, recordval 100 - 199"
    val v5 = "data for person 2, recordval 200 - 299"

    val rec4: (String, String) = ("person1", v4)
    val rec5: (String, String) = ("person2", v5)

    intArr = Array((region2, rec4), (region3, rec5))
    val zipped = sc.parallelize(intArr)


    val newRDD: IntervalRDD[String, String] = testRDD.multiput(zipped)

    var mappedResults: Option[Map[ReferenceRegion, List[(String, String)]]] = newRDD.get(region3)
    var results = mappedResults.get
    println(results)

    val stringWriter = new StringWriter()
    val writer = new PrintWriter(stringWriter)
    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    writer.flush()
    val timings = stringWriter.getBuffer.toString
    println(timings)
    logInfo(timings)

    assert(results.head._2.head._2 == rec5._2)

  }


}
