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

import com.github.akmorrow13.intervaltree._
import org.scalatest._
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.scalatest.Matchers

class IntervalPartitionSuite extends FunSuite  {


	test("create new partition") {
		var partition: IntervalPartition[String, Long, Long] = new IntervalPartition[String, Long, Long]("chr1", new Interval[Long](0L, 0L))
		assert(partition != null)
	}

	test("create partition from iterator") {
		val chr = "chr1"
		val interval1: Interval[Long] = new Interval(0L, 99L)
		val interval2: Interval[Long] = new Interval(100L, 199L)

		val read1 = (1L,2L)
		val read2 = (1L,4L)


		val iter = Iterator(((chr, interval1), read1), ((chr, interval2), read1))
		val partition = IntervalPartition(iter)
		assert(partition != null)
	}

	test("get values from iterator-created partition") {
		val interval1: Interval[Long] = new Interval(0L, 99L)
		val interval2: Interval[Long] = new Interval(100L, 199L)

		val chr1 = "chr1"

		val read1 = (1L,2L)
		val read2 = (1L,500L)
		val read3 = (2L, 2L)
		val read4 =  (2L, 500L)

		val iter = Iterator(((chr1, interval1), read1), ((chr1, interval2), read2), ((chr1, interval1), read3), ((chr1, interval2), read4))
		val partition = IntervalPartition(iter)

		val results = partition.getAll(Iterator(interval1, interval2))
	    for (ku <- results) {
	      if (ku._1.equals(interval1)) {
	      	assert(ku._2.contains(read1))
	      	assert(ku._2.contains(read3))
	      }
	      if (ku._1.equals(interval2)) {
	      	assert(ku._2.contains(read2))
	      	assert(ku._2.contains(read4))
	      }
	    }
	}

	test("put some for iterator of intervals and key-values") {
		val interval1: Interval[Long] = new Interval(0L, 99L)
		val interval2: Interval[Long] = new Interval(100L, 199L)

		val read1 = (1L,2L)
		val read2 = (1L,500L)
		val read3 = (2L, 2L)
		val read4 =  (2L, 500L)

		var partition: IntervalPartition[String, Long, Long] = new IntervalPartition[String, Long, Long]("chr1", new Interval[Long](0L, 0L))
		val iter = Iterator((interval1, List(read1, read3)), (interval2, List(read2, read4)))

		val newPartition = partition.multiput(iter)

		// assert values are in the new partition
		val results = newPartition.getAll(Iterator(interval1, interval2))
	    for (ku <- results) {
	      if (ku._1.equals(interval1)) {
	      	assert(ku._2.contains(read1))
	      	assert(ku._2.contains(read3))
	      }
	      if (ku._1.equals(interval2)) {
	      	assert(ku._2.contains(read2))
	      	assert(ku._2.contains(read4))
	      }
	    }	
	}

	test("get some for iterator of intervals") {
		val interval1: Interval[Long] = new Interval(0L, 99L)
		val interval2: Interval[Long] = new Interval(100L, 199L)

		val chr1 = "chr1"
		val read1 = (1L,2L)
		val read2 = (1L,500L)
		val read3 = (2L, 2L)
		val read4 =  (2L, 500L)

		val iter = Iterator(((chr1, interval1), read1), ((chr1, interval2), read2), ((chr1, interval1), read3), ((chr1, interval2), read4))
		val partition = IntervalPartition(iter)

		val results = partition.multiget(Iterator((interval1, List(1L)),(interval2, List(1L, 2L))))

	    for (ku <- results) {
	    	if (ku._1.equals(interval1)) {
				assert(ku._2.contains(read1))
				assert(!ku._2.contains(read2))				
			}
			if (ku._1.equals(interval2)) {
				assert(ku._2.contains(read2))
				assert(ku._2.contains(read4))
			}
	    }
	}

}
