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

import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.filter2.dsl.Dsl._

import org.bdgenomics.adam.projections.{ Projection, GenotypeField }
import org.apache.parquet.filter2.predicate.FilterPredicate
import com.github.akmorrow13.intervaltree._
import org.scalatest._
import org.apache.spark.{ SparkConf, Logging, SparkContext }
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceRecord, SequenceDictionary, ReferencePosition }
import org.bdgenomics.utils.instrumentation.Metrics
import org.bdgenomics.utils.instrumentation.{RecordedMetrics, MetricsListener}
import org.apache.spark.rdd.MetricsContext._
import java.io.PrintWriter
import java.io.StringWriter
import java.io.OutputStreamWriter
import org.bdgenomics.adam.util.ADAMFunSuite

import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{ AlignmentRecord, Feature, Genotype, GenotypeAllele, NucleotideContigFragment }

class IntervalRDDLoadSuite extends ADAMFunSuite with Logging {

  def getDataFromBamFile(file: String, viewRegion: ReferenceRegion, sampleID: String): RDD[(ReferenceRegion, (String,AlignmentRecord))] = {
    val readsRDD: RDD[(ReferenceRegion, AlignmentRecord)] = sc.loadIndexedBam(file, viewRegion).keyBy(ReferenceRegion(_))
    readsRDD.map(r => (r._1, (sampleID, r._2)))

  }

  def setTime(): Double = {
    System.nanoTime()
  }
  def getTime(start: Double): Double = {
    val time = (System.nanoTime() - start)
    time/1e9
  }

  // sparkTest("Time getting from raw bam file") {
  //   val bamFile = "mouse_chrM.bam"
  //   val region = new ReferenceRegion("chrM", 0L, 1050L)
  //   val key = "person1"
  //
  //   val start = setTime()
  //   // case 1: get data from raw bam file
  //   val alignmentRDD: RDD[(ReferenceRegion, (String,AlignmentRecord))] = getDataFromBamFile(bamFile, region, key)
  //   println(alignmentRDD.count)
  //   val time = getTime(start)
  //   println("Timing for raw bam file")
  //   println(time)
  //   logInfo(time.toString)
  // }

  sparkTest("Time getting from interval rdd") {
    val bamFile = "mouse_chrM.bam"
    val region = new ReferenceRegion("chrM", 0L, 1050L)
    val key = "person1"

    // case 1: get data from raw bam file
    val alignmentRDD: RDD[(ReferenceRegion, (String,AlignmentRecord))] = getDataFromBamFile(bamFile, region, key)

    val sd = new SequenceDictionary(Vector(SequenceRecord("chr", 1000L),
      SequenceRecord("chr2", 1000L),
      SequenceRecord("chrM", 1000L)))

    val metricsListener = new MetricsListener(new RecordedMetrics())
    sc.addSparkListener(metricsListener)
    Metrics.initialize(sc)

    // create intervalrdd from bam file
    var intRDD: IntervalRDD[String, AlignmentRecord] = IntervalRDD(alignmentRDD, sd)

    val start = setTime()

    // case 3: from interval rdd
    val intResults = intRDD.get(region, key)
    //println(intResults.count)

    val time = getTime(start)
    val stringWriter = new StringWriter()
    val writer = new PrintWriter(stringWriter)
    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    writer.flush()
    val timings = stringWriter.getBuffer.toString
    println(timings)

    println("Timings from interval rdd")
    println(time)
    logInfo(time.toString)
  }
  // 
  // sparkTest("Time getting from normal rdd") {
  //   val bamFile = "mouse_chrM.bam"
  //   val region = new ReferenceRegion("chrM", 0L, 1050L)
  //   val key = "person1"
  //
  //   // case 1: get data from raw bam file
  //   val alignmentRDD: RDD[(ReferenceRegion, (String,AlignmentRecord))] = getDataFromBamFile(bamFile, region, key)
  //
  //   // get data from intRDD and alignmentRDD. Compare results
  //   // case 2: from normal rdd
  //   val start = setTime()
  //   val results = alignmentRDD.filter(r => r._1.overlaps(region))
  //   println(results.count)
  //
  //   val time = getTime(start)
  //   println("Timings from normal rdd")
  //   println(time)
  //   logInfo(time.toString)
  // }


}
