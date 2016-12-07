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

package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.{CoGroupedRDD, MapPartitionsRDD}
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.streaming.{Duration, Interval, Time}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class PreReduceWindowDStream[K: ClassTag, V: ClassTag](
                                                        parent: DStream[(K, V)],
                                                        reduceFunc: (V, V) => V,
                                                        _windowDuration: Duration,
                                                        _slideDuration: Duration
                                                      ) extends DStream[(K, V)](parent.ssc) {

  require(_windowDuration.isMultipleOf(parent.slideDuration),
    "The window duration of ReducedWindowedDStream (" + _windowDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  require(_slideDuration.isMultipleOf(parent.slideDuration),
    "The slide duration of ReducedWindowedDStream (" + _slideDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  // Reduce each batch of data using reduceByKey which will be further reduced by window
  // by ReducedWindowedDStream
  private[streaming] def defaultPartitioner(numPartitions: Int = parent.ssc.sc.defaultParallelism) = {
    new HashPartitioner(numPartitions)
  }
  private val reducedStream = parent.reduceByKey(reduceFunc, defaultPartitioner())

  // Persist RDDs to memory by default as these RDDs are going to be reused.
  super.persist(StorageLevel.MEMORY_ONLY_SER)
  reducedStream.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowDuration: Duration = _windowDuration

  override def dependencies: List[DStream[_]] = List(reducedStream)

  override def slideDuration: Duration = _slideDuration

  override val mustCheckpoint = true

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def persist(storageLevel: StorageLevel): DStream[(K, V)] = {
    super.persist(storageLevel)
    reducedStream.persist(storageLevel)
    this
  }

  override def checkpoint(interval: Duration): DStream[(K, V)] = {
    super.checkpoint(interval)
    // reducedStream.checkpoint(interval)
    this
  }

  override def compute(validTime: Time): Option[RDD[(K, V)]] = {


    val currentTime = validTime
    val currentWindow = new Interval(currentTime - windowDuration + parent.slideDuration,
      currentTime - windowDuration + parent.slideDuration+ parent.slideDuration)

    val newRDDs =
    reducedStream.slice(currentWindow)
    logDebug("# old RDDs = " + newRDDs.size)
    Some(newRDDs.head)


  }
}
