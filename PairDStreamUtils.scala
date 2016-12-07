package org.apache.spark.streaming.dstream

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{OutputFormat => NewOutputFormat}
import org.apache.spark.{HashPartitioner, Partitioner, SerializableWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Time}
import org.apache.spark.streaming.StreamingContext.rddToFileName

/**
  * Extra functions available on DStream of (key, value) pairs through an implicit conversion.
  */
class PairDStreamUtils[K, V](self: DStream[(K, V)])
                                (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K])
  extends Serializable {
  private[streaming] def ssc = self.ssc

  private[streaming] def sparkContext = self.context.sparkContext

  private[streaming] def defaultPartitioner(numPartitions: Int = self.ssc.sc.defaultParallelism) = {
    new HashPartitioner(numPartitions)
  }

  def groupByKeydinxiao(partitioner: Partitioner): DStream[(K, Iterable[V])] = ssc.withScope {
    val createCombiner = (v: V) => ArrayBuffer[V](v)
    val mergeValue = (c: ArrayBuffer[V], v: V) => (c += v)
    val mergeCombiner = (c1: ArrayBuffer[V], c2: ArrayBuffer[V]) => (c1 ++ c2)
    combineByKey(createCombiner, mergeValue, mergeCombiner, partitioner)
      .asInstanceOf[DStream[(K, Iterable[V])]]
  }

  def combineByKey[C: ClassTag](
                                 createCombiner: V => C,
                                 mergeValue: (C, V) => C,
                                 mergeCombiner: (C, C) => C,
                                 partitioner: Partitioner,
                                 mapSideCombine: Boolean = true): DStream[(K, C)] = ssc.withScope {
    val cleanedCreateCombiner = sparkContext.clean(createCombiner)
    val cleanedMergeValue = sparkContext.clean(mergeValue)
    val cleanedMergeCombiner = sparkContext.clean(mergeCombiner)
    new ShuffledDStream[K, V, C](
      self,
      cleanedCreateCombiner,
      cleanedMergeValue,
      cleanedMergeCombiner,
      partitioner,
      mapSideCombine)
  }
}
