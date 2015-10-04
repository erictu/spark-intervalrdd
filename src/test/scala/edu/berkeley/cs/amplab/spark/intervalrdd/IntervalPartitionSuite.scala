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
F* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.amplab.spark.intervalrdd

//import scala.collection.immutable.LongMap
//import scala.reflect.ClassTag


//import com.github.akmorrow13.intervaltree._
//import org.scalatest._
//import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.scalatest.Matchers

class IntervalPartitionSuite extends FunSuite  {

  test("setting up environment") {
  	println("setup")
    assert(1 == 0)
  }

 //  // partition testing
 //  test("insert into partition and get values") {
	// var partition: IntervalPartition[Long, Long] = new IntervalPartition[Long, Long]()
	// val read1 = (1L,2L)
	// val read2 = (3L,4L)
	// val interval: Interval[Long] = new Interval(1L, 6L)
	// val iter = Iterator((interval, List(read1)), (interval, List(read2)))
	// partition.multiput(iter)

	// val idList = List(1L, 3L)
	// val srchIter = Iterator((interval, idList))
	// val results = partition.multiget(srchIter)

	// println("results")
	// println(results)
 //  }

 //  test("getting from a partition") {
 //  	val read1 = (1L,2L)
	// val read1 = (3L,4L)
	// val interval: Interval[Long] = new Interval(1L, 6L)
 //  	val iter = Iterator((interval, read1), (interval, read2))
 //  	IntervalPartition(iter)
 //  }

}
