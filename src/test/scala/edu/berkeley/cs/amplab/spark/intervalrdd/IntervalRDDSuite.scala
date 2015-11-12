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

import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{ AlignmentRecord, Feature, Genotype, GenotypeAllele, NucleotideContigFragment }


object TestTimers extends Metrics {
  val Test1 = timer("Test1")
  val Test2 = timer("Test2")
  val Test3 = timer("Test3")
}


class IntervalRDDSuite extends ADAMFunSuite with Logging {


    sparkTest("assert the data pulled from a file is the same") {
      val metricsListener = new MetricsListener(new RecordedMetrics())
      sc.addSparkListener(metricsListener)
      Metrics.initialize(sc)


      val bamFile = "./mouse_chrM.bam"
      val region = new ReferenceRegion("chrM", 0L, 5000L)
      var loadedBam = sc.loadIndexedBam(bamFile, region).instrument
      val data = Array((region, ("person1", loadedBam.collect.toList)))
      val rdd = sc.parallelize(data).instrument
      var dict = sc.adamDictionaryLoad[AlignmentRecord](bamFile)
      var intRDD = IntervalRDD(rdd, dict)
      var startTime = System.currentTimeMillis
      var results = intRDD.get(region, "person1")
      var endTime = System.currentTimeMillis
      var diff = (endTime - startTime)
      println("Initial query1: " + diff)
      println()

      TestTimers.Test1.time{
      startTime = System.currentTimeMillis
      results = intRDD.get(region, "person1")
      endTime = System.currentTimeMillis
      diff = (endTime - startTime)
      println("query2: " + diff)
      println()

      startTime = System.currentTimeMillis
      intRDD.get(region, "person1")
      endTime = System.currentTimeMillis
      diff = (endTime - startTime)
      println("query3: " + diff)
      println()

      startTime = System.currentTimeMillis
      intRDD.get(region, "person1")
      endTime = System.currentTimeMillis
      diff = (endTime - startTime)
      println("query4: " + diff)
      println()

      startTime = System.currentTimeMillis
      intRDD.get(region, "person1")
      endTime = System.currentTimeMillis
      diff = (endTime - startTime)
      println("query5: " + diff)
      println()
      }

      startTime = System.currentTimeMillis
      loadedBam = sc.loadIndexedBam(bamFile, region)
      endTime = System.currentTimeMillis
      diff = (endTime - startTime)
      println("loadIndexedBamDirectly: " + diff)

      startTime = System.currentTimeMillis
      sc.loadBam(bamFile).filterByOverlappingRegion(region)
      endTime = System.currentTimeMillis
      diff = (endTime - startTime)
      println("loadBamDirectly: " + diff)

      // println(loadedBam.collect.toList.size)
      // println(results.get.get(region).get(0)._2.length)
      // assert(loadedBam.collect.toList.size == results.get.get(region).get(0)._2.length)

      val stringWriter = new StringWriter()
      val writer = new PrintWriter(stringWriter)
      Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
      writer.flush()
      val timings = stringWriter.getBuffer.toString
      println(timings)
      logInfo(timings)

    }

  // sparkTest("create IntervalRDD from RDD using apply") {
  //   val metricsListener = new MetricsListener(new RecordedMetrics())
  //   sc.addSparkListener(metricsListener)
  //   Metrics.initialize(sc)

  //   val chr1 = "chr1"
  //   val chr2 = "chr2"
  //   val chr3 = "chr3"
  //   val region1: ReferenceRegion = new ReferenceRegion(chr1, 0L, 99L)
  //   val region2: ReferenceRegion = new ReferenceRegion(chr2, 100L, 199L)
  //   val region3: ReferenceRegion = new ReferenceRegion(chr3, 200L, 299L)

  //   //creating data
  //   val rec1: (String, String) = ("person1", "data for person 1, recordval1 0-99")
  //   val rec2: (String, String) = ("person2", "data for person 2, recordval2 100-199")
  //   val rec3: (String, String) = ("person3", "data for person 3, recordval3 200-299")

  //   var intArr = Array((region1, rec1), (region2, rec2), (region3, rec3))
  //   var intArrRDD: RDD[(ReferenceRegion, (String, String))] = sc.parallelize(intArr)

  //   val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
  //     SequenceRecord("chr2", 1000L), 
  //     SequenceRecord("chr3", 1000L))) //NOTE: the number is the length of the chromosome

  //   var testRDD: IntervalRDD[String, String] = IntervalRDD(intArrRDD, sd)
  //   assert(1 == 1)

  //   val stringWriter = new StringWriter()
  //   val writer = new PrintWriter(stringWriter)
  //   Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
  //   writer.flush()
  //   val timings = stringWriter.getBuffer.toString
  //   println(timings)
  //   logInfo(timings)

  // }

  // sparkTest("get one interval, k value") {
  //   val metricsListener = new MetricsListener(new RecordedMetrics())
  //   sc.addSparkListener(metricsListener)
  //   Metrics.initialize(sc)

  //   val chr1 = "chr1"
  //   val chr2 = "chr2"
  //   val chr3 = "chr3"
  //   val region1: ReferenceRegion = new ReferenceRegion(chr1, 0L, 99L)
  //   val region2: ReferenceRegion = new ReferenceRegion(chr2, 100L, 199L)
  //   val region3: ReferenceRegion = new ReferenceRegion(chr3, 200L, 299L)

  //   //creating data
  //   val rec1: (String, String) = ("person1", "data for person 1: recordval1 0-99")
  //   val rec2: (String, String) = ("person2", "data for person 2: recordval2 100-199")
  //   val rec3: (String, String) = ("person3", "data for person 3: recordval3 200-299")

  //   var intArr = Array((region1, rec1), (region2, rec2), (region3, rec3))
  //   var intArrRDD: RDD[(ReferenceRegion, (String, String))] = sc.parallelize(intArr)

  //   //See TODO flags above
  //   //initializing IntervalRDD with certain values
  //   val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
  //     SequenceRecord("chr2", 1000L),
  //     SequenceRecord("chr3", 1000L)))

  //   var testRDD: IntervalRDD[String, String] = IntervalRDD(intArrRDD, sd)
  //   // testRDD.persist()
  //   // testRDD.count()
    
  //   // var mappedResults: Option[Map[ReferenceRegion, List[(String, String)]]] = testRDD.get(region1, "person1")
  //   // var results = mappedResults.get
  //   // assert(results.get(region1).get.size == 1)
  //   TestTimers.Test1.time {
  //   var mappedResults = testRDD.get(region1)
  //   mappedResults = testRDD.get(region1)
  //   mappedResults = testRDD.get(region1)
  //   mappedResults = testRDD.get(region1)
  //   mappedResults = testRDD.get(region1)
  //   // mappedResults = testRDD.get(region1)
  //   // mappedResults = testRDD.get(region1)
  //   // mappedResults = testRDD.get(region1)
  //   // mappedResults = testRDD.get(region1)
  //   // mappedResults = testRDD.get(region1)
  //   // mappedResults = testRDD.get(region1)
  //   println(mappedResults.get)
  //   }

  //   // TestTimers.Test2.time {
  //   // var mappedResults2 = testRDD.get(region2)
  //   // mappedResults2 = testRDD.get(region2)
  //   // mappedResults2 = testRDD.get(region2)
  //   // mappedResults2 = testRDD.get(region2)
  //   // mappedResults2 = testRDD.get(region2)
  //   // mappedResults2 = testRDD.get(region2)
  //   // mappedResults2 = testRDD.get(region2)
  //   // mappedResults2 = testRDD.get(region2)
  //   // mappedResults2 = testRDD.get(region2)
  //   // mappedResults2 = testRDD.get(region2)
  //   // mappedResults2 = testRDD.get(region2)
  //   // println(mappedResults2.get)
  //   // // var results = mappedResults.get
  //   // }

  //   // TestTimers.Test3.time {
  //   // var mappedResults3 = testRDD.get(region3)
  //   // mappedResults3 = testRDD.get(region3)
  //   // mappedResults3 = testRDD.get(region3)
  //   // mappedResults3 = testRDD.get(region3)
  //   // mappedResults3 = testRDD.get(region3)
  //   // mappedResults3 = testRDD.get(region3)
  //   // mappedResults3 = testRDD.get(region3)
  //   // mappedResults3 = testRDD.get(region3)
  //   // mappedResults3 = testRDD.get(region3)
  //   // mappedResults3 = testRDD.get(region3)
  //   // mappedResults3 = testRDD.get(region3)
  //   // println(mappedResults3.get)
  //   // // var results = mappedResults.get
  //   // }

  //   // assert(results.get(region3).get.size == 1)

  //   assert(0 == 0)
  //   val stringWriter = new StringWriter()
  //   val writer = new PrintWriter(stringWriter)
  //   Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
  //   writer.flush()
  //   val timings = stringWriter.getBuffer.toString
  //   println(timings)
  //   logInfo(timings)
  // }

  // sparkTest("put multiple keys to one chromosome. Test for key specificity") {
  //   val metricsListener = new MetricsListener(new RecordedMetrics())
  //   sc.addSparkListener(metricsListener)
  //   Metrics.initialize(sc)

  //   val chr1 = "chr1"
  //   val region1: ReferenceRegion = new ReferenceRegion(chr1, 0L, 99L)

  //   //creating data
  //   val rec1: (String, String) = ("person1", "data for person 1- recordval1 0-99")
  //   val rec2: (String, String) = ("person2", "data for person 2- recordval2 0-99")
  //   val rec3: (String, String) = ("person3", "data for person 3 recordval3 0-99")

  //   var intArr = Array((region1, rec1), (region1, rec2), (region1, rec3))
  //   var intArrRDD: RDD[(ReferenceRegion, (String, String))] = sc.parallelize(intArr)

  //   //See TODO flags above
  //   //initializing IntervalRDD with certain values
  //   val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
  //     SequenceRecord("chr2", 1000L),
  //     SequenceRecord("chr3", 1000L)))

  //   var testRDD: IntervalRDD[String, String] = IntervalRDD(intArrRDD, sd)
    
  //   var mappedResults: Option[Map[ReferenceRegion, List[(String, String)]]] = testRDD.get(region1)
  //   mappedResults = testRDD.get(region1)
  //   mappedResults = testRDD.get(region1)
  //   mappedResults = testRDD.get(region1)
  //   mappedResults = testRDD.get(region1)
  //   var results = mappedResults.get
  //   assert(results.get(region1).get.size == 3)

  //   val stringWriter = new StringWriter()
  //   val writer = new PrintWriter(stringWriter)
  //   Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
  //   writer.flush()
  //   val timings = stringWriter.getBuffer.toString
  //   println(timings)
  //   logInfo(timings)

  // }

  // sparkTest("put multiple intervals into RDD to existing chromosome") {
  //   val metricsListener = new MetricsListener(new RecordedMetrics())
  //   sc.addSparkListener(metricsListener)
  //   Metrics.initialize(sc)

  //   val chr1 = "chr1"
  //   val chr2 = "chr2"
  //   val chr3 = "chr3"
  //   val region1: ReferenceRegion = new ReferenceRegion(chr1, 0L, 99L)
  //   val region2: ReferenceRegion = new ReferenceRegion(chr2, 100L, 199L)
  //   val region3: ReferenceRegion = new ReferenceRegion(chr3, 200L, 299L)

  //   //creating data
  //   val rec1: (String, String) = ("person1", "data for person 1, recordval1 0-99")
  //   val rec2: (String, String) = ("person2", "data for person 2, recordval2 100-199")
  //   val rec3: (String, String) = ("person3", "data for person 3, recordval3 200-299")

  //   var intArr = Array((region1, rec1), (region2, rec2), (region3, rec3))
  //   var intArrRDD: RDD[(ReferenceRegion, (String, String))] = sc.parallelize(intArr)

  //   //See TODO flags above
  //   //initializing IntervalRDD with certain values
  //   val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
  //     SequenceRecord("chr2", 1000L),
  //     SequenceRecord("chr3", 1000L)))

  //   var testRDD: IntervalRDD[String, String] = IntervalRDD(intArrRDD, sd)


  //   val v4 = "data for person 1, recordval 100 - 199"
  //   val v5 = "data for person 2, recordval 200 - 299"

  //   val rec4: (String, String) = ("person1", v4)
  //   val rec5: (String, String) = ("person2", v5)

  //   intArr = Array((region2, rec4), (region3, rec5))
  //   val zipped = sc.parallelize(intArr)


  //   val newRDD: IntervalRDD[String, String] = testRDD.multiput(zipped, sd)

  //   var mappedResults: Option[Map[ReferenceRegion, List[(String, String)]]] = newRDD.get(region3)
  //   mappedResults = newRDD.get(region3)
  //   mappedResults = newRDD.get(region3)
  //   mappedResults = newRDD.get(region3)
  //   mappedResults = newRDD.get(region3)
  //   var results = mappedResults.get
  //   assert(results.head._2.head._2 == rec5._2)

  //   val stringWriter = new StringWriter()
  //   val writer = new PrintWriter(stringWriter)
  //   Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
  //   writer.flush()
  //   val timings = stringWriter.getBuffer.toString
  //   println(timings)
  //   logInfo(timings)

  // }


  // sparkTest("call put multiple times on reads with same ReferenceRegion") {
  //   val metricsListener = new MetricsListener(new RecordedMetrics())
  //   sc.addSparkListener(metricsListener)
  //   Metrics.initialize(sc)

  //   val region: ReferenceRegion = new ReferenceRegion("chr1", 0L, 99L)

  //   //creating data
  //   val rec1: (String, String) = ("per1", "p1 0-99")
  //   val rec2: (String, String) = ("per2", "p2 100-199")
  //   val rec3: (String, String) = ("per3", "p3 200-299")
  //   val rec4: (String, String) = ("per4", "p4 100-199")
  //   val rec5: (String, String) = ("per5", "p5 200-299")
  //   val rec6: (String, String) = ("per6", "p6 300-399")

  //   var intArr = Array((region, rec1), (region, rec2), (region, rec3))
  //   var intArrRDD: RDD[(ReferenceRegion, (String, String))] = sc.parallelize(intArr)

  //   val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L)))

  //   var testRDD: IntervalRDD[String, String] = IntervalRDD(intArrRDD, sd)

  //   //constructing RDDs to put in
  //   val onePutInput = Array((region, rec4), (region, rec5))
  //   val zipped: RDD[(ReferenceRegion, (String, String))] = sc.parallelize(onePutInput)

  //   val twoPutInput = Array((region, rec6))
  //   val zipped2: RDD[(ReferenceRegion, (String, String))] = sc.parallelize(twoPutInput)

  //   // Call Put twice
  //   val onePutRDD: IntervalRDD[String, String] = testRDD.multiput(zipped, sd)
  //   val twoPutRDD: IntervalRDD[String, String] = onePutRDD.multiput(zipped2, sd)


  //   var resultsOrig: Option[Map[ReferenceRegion, List[(String, String)]]] = testRDD.get(region)
  //   var resultsOne: Option[Map[ReferenceRegion, List[(String, String)]]] = onePutRDD.get(region)
  //   var resultsTwo: Option[Map[ReferenceRegion, List[(String, String)]]] = twoPutRDD.get(region)

  //   assert(resultsOrig.get.head._2.size == 3) //size of results for the one region we queried
  //   assert(resultsOne.get.head._2.size == 5) //size after adding two records
  //   assert(resultsTwo.get.head._2.size == 6) //size after adding another record

  //   val stringWriter = new StringWriter()
  //   val writer = new PrintWriter(stringWriter)
  //   Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
  //   writer.flush()
  //   val timings = stringWriter.getBuffer.toString
  //   println(timings)
  //   logInfo(timings)

  // }

  // sparkTest("merge RDDs across multiple chromosomes") {
  //   val metricsListener = new MetricsListener(new RecordedMetrics())
  //   sc.addSparkListener(metricsListener)
  //   Metrics.initialize(sc)

  //   val region1: ReferenceRegion = new ReferenceRegion("chr1", 0L, 99L)
  //   val region2: ReferenceRegion = new ReferenceRegion("chr2", 0L,  199L)

  //   //creating data
  //   val rec1: (String, String) = ("per1", "p1 chr1 0-99")
  //   val rec2: (String, String) = ("per2", "p2 chr1 0-99")
  //   val rec3: (String, String) = ("per3", "p3 chr1 0-99")
  //   val rec4: (String, String) = ("per1", "p1 chr2 0-99")
  //   val rec5: (String, String) = ("per2", "p2 chr2 0-99")
  //   val rec6: (String, String) = ("per3", "p3 chr2 0-99")

  //   var intArr = Array((region1, rec1), (region1, rec2), (region1, rec3))
  //   var intArrRDD: RDD[(ReferenceRegion, (String, String))] = sc.parallelize(intArr)

  //   val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
  //     SequenceRecord("chr2", 1000L)))

  //   var testRDD: IntervalRDD[String, String] = IntervalRDD(intArrRDD, sd)

  //   val newRDD = Array((region2, rec4), (region2, rec5))
  //   val zipped: RDD[(ReferenceRegion, (String, String))] = sc.parallelize(newRDD)


  //   val chr2RDD: IntervalRDD[String, String] = testRDD.multiput(zipped, sd)

  //   var origChr1: Option[Map[ReferenceRegion, List[(String, String)]]] = testRDD.get(region1)
  //   var newChr1: Option[Map[ReferenceRegion, List[(String, String)]]] = chr2RDD.get(region1)

  //   assert(origChr1 == newChr1) 

  //   var newChr2: Option[Map[ReferenceRegion, List[(String, String)]]] = chr2RDD.get(region2)

  //   assert(newChr2.get.head._2.size == newRDD.size) 
  //   val stringWriter = new StringWriter()
  //   val writer = new PrintWriter(stringWriter)
  //   Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
  //   writer.flush()
  //   val timings = stringWriter.getBuffer.toString
  //   println(timings)
  //   logInfo(timings)

  // }

}
