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

class IntervalRDDSuite extends FunSuite  {

  var conf = new SparkConf(false)
  var sc = new SparkContext("local", "test", conf)

  test("Small Test") {
    
    //creating intervals
    val int1: Interval[Long] = new Interval(0L, 99L)
    val int2: Interval[Long] = new Interval(100L, 199L)
    val int3: Interval[Long] = new Interval(200L, 299L)
    var intArr: Array[Interval[Long]] = Array(int1, int2, int3)
    var intArrRDD: RDD[Interval[Long]] = sc.parallelize(intArr)

    //creating data
    val rec1: (String, String) = ("chr1", "recordval1 0-99")
    val rec2: (String, String) = ("chr2", "recordval2 100-199")
	val rec3: (String, String) = ("chr3", "recordval3 200-299")
	var recArr: Array[(String, String)] = Array(rec1, rec2, rec3)
	var recArrRDD: RDD[(String, String)] = sc.parallelize(recArr)
	var zipped: RDD[(Interval[Long], (String, String))] = intArrRDD.zip(recArrRDD)

	//initializing IntervalRDD with certain values
	var testRDD: IntervalRDD[Interval[Long], String, String] = IntervalRDD(zipped)

	//creating stuff to insert into our IntervalRDD
	var keyRDD: RDD[Interval[Long]] = sc.parallelize(Array(new Interval(300L, 399L)))
    val insert1: (String, String) = ("chr1", "insertordval1 300-399")
    val insert2: (String, String) = ("chr2", "insertordval2 300-399")
	val insert3: (String, String) = ("chr3", "insertordval3 300-399")
	var insertArr: Array[(String, String)] = Array(insert1, insert2, insert3)
	var insertArrRDD: RDD[(String, String)] = sc.parallelize(insertArr)
	var insertZipped: RDD[(Interval[Long], (String, String))] = keyRDD.zip(insertArrRDD)
	testRDD.multiput("chr1", new Interval(300L, 399L), insertZipped)
  }

}
