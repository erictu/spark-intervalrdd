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
import org.apache.parquet.filter2.dsl.Dsl._

import org.bdgenomics.adam.projections.{ Projection, GenotypeField }
import org.apache.parquet.filter2.predicate.FilterPredicate
import com.github.erictu.intervaltree._
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

object TestTimers extends Metrics {
  val Test1 = timer("Test1")
  val Test2 = timer("Test2")
  val Test3 = timer("Test3")
}


class IntervalRDDSuite extends ADAMFunSuite with Logging {

  sparkTest("Get data") {
    val bamFile = "./mouse_chrM_p1.bam"

      val region = new ReferenceRegion("chrM", 0L, 10000L)
      val sd = new SequenceDictionary(Vector(SequenceRecord("chrM", 1000L)))
      val rdd: RDD[AlignmentRecord] = sc.loadIndexedBam(bamFile, region)
      val alignmentRDD: RDD[(ReferenceRegion, AlignmentRecord)] = rdd.map(v => (region, v))

      var intRDD: IntervalRDD[AlignmentRecord] = IntervalRDD(alignmentRDD, sd)
      val results = intRDD.get(region)
      assert(results.size != 0)
      println(results.size)

  }

//
//   sparkTest("create IntervalRDD from RDD using apply") {
//     val metricsListener = new MetricsListener(new RecordedMetrics())
//     sc.addSparkListener(metricsListener)
//     Metrics.initialize(sc)
//
//     val chr1 = "chr1"
//     val chr2 = "chr2"
//     val chr3 = "chr3"
//     val region1: ReferenceRegion = new ReferenceRegion(chr1, 0L, 99L)
//     val region2: ReferenceRegion = new ReferenceRegion(chr2, 100L, 199L)
//     val region3: ReferenceRegion = new ReferenceRegion(chr3, 200L, 299L)
//
//     //creating data
//     val rec1 = "data1"
//     val rec2 = "data2"
//     val rec3 = "data3"
//
//     var intArr = Array((region1, rec1), (region2, rec2), (region3, rec3))
//     var intArrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(intArr)
//
//     val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
//       SequenceRecord("chr2", 1000L),
//       SequenceRecord("chr3", 1000L))) //NOTE: the number is the length of the chromosome
//
//     var testRDD: IntervalRDD[String] = IntervalRDD(intArrRDD, sd)
//
//   }
//
//   sparkTest("get one interval, k value") {
//     val metricsListener = new MetricsListener(new RecordedMetrics())
//     sc.addSparkListener(metricsListener)
//     Metrics.initialize(sc)
//
//     val chr1 = "chr1"
//     val chr2 = "chr2"
//     val chr3 = "chr3"
//     val region1: ReferenceRegion = new ReferenceRegion(chr1, 0L, 99L)
//     val region2: ReferenceRegion = new ReferenceRegion(chr2, 100L, 199L)
//     val region3: ReferenceRegion = new ReferenceRegion(chr3, 200L, 299L)
//
//     //creating data
//     val rec1 = "data1"
//     val rec2 = "data2"
//     val rec3 = "data3"
//
//     var intArr = Array((region1, rec1), (region2, rec2), (region3, rec3))
//     var intArrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(intArr)
//
//     //See TODO flags above
//     //initializing IntervalRDD with certain values
//     val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
//       SequenceRecord("chr2", 1000L),
//       SequenceRecord("chr3", 1000L)))
//
//     var testRDD: IntervalRDD[String] = IntervalRDD(intArrRDD, sd)
//     assert(testRDD.get(region1).size == 1)
//
//
//   }
//
//   sparkTest("put multiple intervals into RDD to existing chromosome") {
//     val metricsListener = new MetricsListener(new RecordedMetrics())
//     sc.addSparkListener(metricsListener)
//     Metrics.initialize(sc)
//
//     val chr1 = "chr1"
//     val chr2 = "chr2"
//     val chr3 = "chr3"
//     val region1: ReferenceRegion = new ReferenceRegion(chr1, 0L, 99L)
//     val region2: ReferenceRegion = new ReferenceRegion(chr2, 100L, 199L)
//     val region3: ReferenceRegion = new ReferenceRegion(chr3, 200L, 299L)
//
//     //creating data
//     val rec1 = "data1"
//     val rec2 = "data2"
//     val rec3 = "data3"
//
//     var intArr = Array((region1, rec1), (region2, rec2), (region3, rec3))
//     var intArrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(intArr)
//
//     //See TODO flags above
//     //initializing IntervalRDD with certain values
//     val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
//       SequenceRecord("chr2", 1000L),
//       SequenceRecord("chr3", 1000L)))
//
//     var testRDD: IntervalRDD[String] = IntervalRDD(intArrRDD, sd)
//
//     val rec4 = "data4"
//     val rec5 = "data5"
//
//     intArr = Array((region2, rec4), (region3, rec5))
//     val zipped = sc.parallelize(intArr)
//
//
//     val newRDD: IntervalRDD[String] = testRDD.multiput(zipped, sd)
//
//     var results = newRDD.get(region3)
//     assert(results.size == 2)
//   }
//
//   sparkTest("call put multiple times on reads with same ReferenceRegion") {
//     val metricsListener = new MetricsListener(new RecordedMetrics())
//     sc.addSparkListener(metricsListener)
//     Metrics.initialize(sc)
//
//     val region: ReferenceRegion = new ReferenceRegion("chr1", 0L, 99L)
//
//     //creating data
//     val rec1 = "data1"
//     val rec2 = "data2"
//     val rec3 = "data3"
//     val rec4 = "data4"
//     val rec5 = "data5"
//     val rec6 = "data6"
//
//     var intArr = Array((region, rec1), (region, rec2), (region, rec3))
//     var intArrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(intArr)
//
//     val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L)))
//
//     var testRDD: IntervalRDD[String] = IntervalRDD(intArrRDD, sd)
//
//     //constructing RDDs to put in
//     val onePutInput = Array((region, rec4), (region, rec5))
//     val zipped: RDD[(ReferenceRegion, String)] = sc.parallelize(onePutInput)
//
//     val twoPutInput = Array((region, rec6))
//     val zipped2: RDD[(ReferenceRegion, String)] = sc.parallelize(twoPutInput)
//
//     // Call Put twice
//     val onePutRDD: IntervalRDD[String] = testRDD.multiput(zipped, sd)
//     val twoPutRDD: IntervalRDD[String] = onePutRDD.multiput(zipped2, sd)
//
//
//     var resultsOrig = testRDD.get(region)
//     var resultsOne = onePutRDD.get(region)
//     var resultsTwo = twoPutRDD.get(region)
//
//     assert(resultsOrig.size == 3) //size of results for the one region we queried
//     assert(resultsOne.size == 5) //size after adding two records
//     assert(resultsTwo.size == 6) //size after adding another record
//
//   }
//
//   sparkTest("merge RDDs across multiple chromosomes") {
//     val metricsListener = new MetricsListener(new RecordedMetrics())
//     sc.addSparkListener(metricsListener)
//     Metrics.initialize(sc)
//
//     val region1: ReferenceRegion = new ReferenceRegion("chr1", 0L, 99L)
//     val region2: ReferenceRegion = new ReferenceRegion("chr2", 0L,  199L)
//
//     //creating data
//     val rec1 = "data1"
//     val rec2 = "data2"
//     val rec3 = "data3"
//     val rec4 = "data4"
//     val rec5 = "data5"
//     val rec6 = "data6"
//
//     var intArr = Array((region1, rec1), (region1, rec2), (region1, rec3))
//     var intArrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(intArr)
//
//     val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
//       SequenceRecord("chr2", 1000L)))
//
//     var testRDD: IntervalRDD[String] = IntervalRDD(intArrRDD, sd)
//
//     val newRDD = Array((region2, rec4), (region2, rec5))
//     val zipped: RDD[(ReferenceRegion, String)] = sc.parallelize(newRDD)
//
//
//     val chr2RDD: IntervalRDD[String] = testRDD.multiput(zipped, sd)
//
//     var origChr1 = testRDD.get(region1)
//     var newChr1 = chr2RDD.get(region1)
//
//     assert(origChr1 == newChr1)
//
//     var newChr2 = chr2RDD.get(region2)
//
//     assert(newChr2.size == newRDD.size)
//   }
//
//   sparkTest("test for vcf data retreival") {
//
//     val filePath = "./6-sample.vcf"
//
//     val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L)))
//
//     // load variant data
//     val chr1 = "chr1"
//     val viewRegion: ReferenceRegion = new ReferenceRegion(chr1, 0L, 99L)
//
//     val vRDD: RDD[Genotype] = sc.loadGenotypes(filePath).filterByOverlappingRegion(viewRegion)
//     val variantRDD: RDD[(ReferenceRegion, Genotype)] = vRDD.keyBy(v => ReferenceRegion(ReferencePosition(v)))
//
//     var testRDD: IntervalRDD[Genotype] = IntervalRDD(variantRDD, sd)
//     val results = testRDD.get(viewRegion)
//     println(results)
//
//   }
//
//
//   sparkTest("test new rdd") {
//
//     val region: ReferenceRegion = new ReferenceRegion("chr1", 0L, 99L)
//
//     //creating data
//     val rec1 = "data1"
//     val rec2 = "data2"
//     val rec3 = "data3"
//     val rec4 = "data4"
//     val rec5 = "data5"
//     val rec6 = "data6"
//
//     var intArr = Array((region, rec1), (region, rec2), (region, rec3))
//     var intArrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(intArr)
//
//     val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L)))
//
//     var testRDD: IntervalRDD[String] = IntervalRDD(intArrRDD, sd)
//
//     val r: ReferenceRegion = new ReferenceRegion("chr1", 0L, 100L)
//     val newRDD = testRDD.filterByRegion(r)
//     println(newRDD.count)
//     println(testRDD.count)
//
//   }

}
