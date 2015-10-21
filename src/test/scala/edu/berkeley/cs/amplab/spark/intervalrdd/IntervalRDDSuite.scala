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
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.bdgenomics.adam.models.ReferenceRegion

class IntervalRDDSuite extends FunSuite  {

  var conf = new SparkConf(false)
  var sc = new SparkContext("local", "test", conf)
  val partitions = 100 

  test("create IntervalRDD from RDD using apply") {
    //creating intervals

    val chr1 = "chr1"
    val chr2 = "chr2"
    val chr3 = "chr3"
    val region1: ReferenceRegion = new ReferenceRegion(chr1, 0L, 99L)
    val region2: ReferenceRegion = new ReferenceRegion(chr2, 100L, 199L)
    val region3: ReferenceRegion = new ReferenceRegion(chr3, 200L, 299L)

    val k1 = (chr1, region3)
    val k2 = (chr2, region3)
    val k3 = (chr3, region3)

    var intArr = Array(k1, k2, k3)
    var intArrRDD: RDD[(String, ReferenceRegion)] = sc.parallelize(intArr)

    //creating data
    val rec1: (String, String) = ("person1", "data for person 1, recordval1 0-99")
    val rec2: (String, String) = ("person2", "data for person 2, recordval2 100-199")
    val rec3: (String, String) = ("person3", "data for person 3, recordval3 200-299")
    var recArr: Array[(String, String)] = Array(rec1, rec2, rec3)
    var recArrRDD: RDD[(String, String)] = sc.parallelize(recArr)
    var zipped: RDD[((String, ReferenceRegion), (String, String))] = intArrRDD.zip(recArrRDD)

    //initializing IntervalRDD with certain values
    var testRDD: IntervalRDD[ReferenceRegion, String, String] = IntervalRDD(zipped)

    assert(1 == 1)

  }

  test("get one interval, k value") {

    val chr1 = "chr1"
    val chr2 = "chr2"
    val chr3 = "chr3"

    val region1: ReferenceRegion = new ReferenceRegion(chr1, 0L, 99L)
    val region2: ReferenceRegion = new ReferenceRegion(chr2, 100L, 199L)
    val region3: ReferenceRegion = new ReferenceRegion(chr3, 200L, 299L)

    val k1 = (chr1, region1)
    val k2 = (chr2, region2)
    val k3 = (chr3, region3)

    var intArr = Array(k1, k2, k3)
    var intArrRDD: RDD[(String, ReferenceRegion)] = sc.parallelize(intArr, 3)

    //creating data
    val v1 = "data for person 1, recordval1 100-199"
    val v2 = "data for person 2, recordval2 100-199"
    val v3 = "data for person 3, recordval3 200-299"

    val rec1: (String, String) = ("person1", v1)
    val rec2: (String, String) = ("person2", v2)
    val rec3: (String, String) = ("person3", v3)

    var recArr: Array[(String, String)] = Array(rec1, rec2, rec3)
    var recArrRDD: RDD[(String, String)] = sc.parallelize(recArr, 3)
    var zipped: RDD[((String, ReferenceRegion), (String, String))] = intArrRDD.zip(recArrRDD)

    //initializing IntervalRDD with certain values
    var testRDD: IntervalRDD[ReferenceRegion, String, String] = IntervalRDD(zipped)
    
    var mappedResults: Option[Map[ReferenceRegion, List[(String, String)]]] = testRDD.get("chr1", region2)
    var results = mappedResults.get
    assert(results.head._2.head._2 == v1)

    mappedResults = testRDD.get("chr3", region3)
    results = mappedResults.get
    assert(results.head._2.head._2 == v3)

  }

  test("put multiple intervals into RDD to existing chromosome") {

    val chr1 = "chr1"
    val chr2 = "chr2"
    val chr3 = "chr3"

    val region1: ReferenceRegion = new ReferenceRegion(chr1, 0L, 99L)
    val region2: ReferenceRegion = new ReferenceRegion(chr2, 100L, 199L)
    val region3: ReferenceRegion = new ReferenceRegion(chr3, 200L, 299L)

    val k1 = (chr1, region1)
    val k2 = (chr2, region2)
    val k3 = (chr3, region3)

    var intArr = Array(k1, k2, k3)
    var intArrRDD: RDD[(String, ReferenceRegion)] = sc.parallelize(intArr)

    //creating data
    val v1 = "data for person 1, recordval1 0-99"
    val v2 = "data for person 2, recordval2 100-199"
    val v3 = "data for person 3, recordval3 200-299"

    val rec1: (String, String) = ("person1", v1)
    val rec2: (String, String) = ("person2", v2)
    val rec3: (String, String) = ("person3", v3)

    var recArr: Array[(String, String)] = Array(rec1, rec2, rec3)
    var recArrRDD: RDD[(String, String)] = sc.parallelize(recArr)
    var zipped: RDD[((String, ReferenceRegion), (String, String))] = intArrRDD.zip(recArrRDD)

    //initializing IntervalRDD with certain values
    var testRDD: IntervalRDD[ReferenceRegion, String, String] = IntervalRDD(zipped)

    intArr = Array(("chr1", region2), ("chr2", region3))
    intArrRDD = sc.parallelize(intArr)

    val v4 = "data for person 1, recordval 100 - 199"
    val v5 = "data for person 2, recordval 200 - 299"

    val rec4: (String, String) = ("person1", v4)
    val rec5: (String, String) = ("person2", v5)

    recArr = Array(rec4, rec5)
    recArrRDD = sc.parallelize(recArr)
    zipped = intArrRDD.zip(recArrRDD)

    val newRDD: IntervalRDD[ReferenceRegion, String, String] = testRDD.multiput(zipped)

    var mappedResults: Option[Map[ReferenceRegion, List[(String, String)]]] = newRDD.get("chr3", region3)
    var results = mappedResults.get
    println(results)
    assert(results.head._2.head._2 == v5)
  }


}
