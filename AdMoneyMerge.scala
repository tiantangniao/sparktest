package com.bj58

import com.bj58.javautils.{JsonData, KafkaRealtimeSearchOdsProducer}
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.joda.time.DateTime

import scala.collection.JavaConverters._

/**
  * Created by 58 on 2016/12/06.
  */

object AdMoneyMerge {
  def adSerchConertToArray(strline: String): Array[String] = {
    val linestrWlist = strline.split("\001")
    if (linestrWlist.length == 24) {
      val result: Array[String] = new Array[String](1)
      result(0) = strline
      result
    } else if (linestrWlist.length == 26) {
      val linestrWlistPre: Array[String] = new Array(25)
      linestrWlist.copyToArray(linestrWlistPre, 0, 25)
      val srtPre: String = linestrWlistPre.mkString("\001");
      val ad: String = linestrWlist(25)
      val adinfolost = JsonData.adSerchStringToJSONArray(ad)
      val result: Array[String] = new Array[String](adinfolost.length)
      var i: Int = 0
      for (item <- adinfolost) {
        result(i) = srtPre + "\001" + item
        i = i + 1
      }
      result
    } else {
      val result: Array[String] = new Array[String](1)
      result(0) = "-"
      result
    }
  }


  def splList(list: List[List[String]]): List[String] = {
    var subList: List[String] = List();
    for (a <- list) {
      if (a != null) {
        subList ::: (a)
      }
    }
    subList
  }


  def anotherAdLog(): DStream[(String, String)] = {
    val (zkQuorum, groupId, topics, numThreads) = (
      "10.126.99.105:2181,10.126.99.196:2181,10.126.81.208:2181,10.126.100.144:2181,10.126.81.215:2181/58_kafka_cluster",
      "hdp_lbg_ectech_lm_ods_click_seaech_ceshi",
      "hdp_lbg_ectech_lm_ods_click,hdp_lbg_ectech_lm_ods_search",
      1)
    val kafkaParams = Map[String, String]("zookeeper.connect" -> "10.126.99.105:2181,10.126.99.196:2181,10.126.81.208:2181,10.126.100.144:2181,10.126.81.215:2181/58_kafka_cluster",
      "group.id" -> "hdp_lbg_ectech_lm_ods_click_seaech_ceshi", "zookeeper.connection.timeout" -> "10000")
    val numInputStream = 4
    val ssc = new StreamingContext(new SparkConf().set("spark.kryoserializer.buffer.max", "1024m").setAppName("sparkstudy"), Seconds(360))
    val topicMap = topics.split(",").map((_, numThreads)).toMap
    val kafkaDstreams = (1 to numInputStream).map {
      _ => KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    }
    val lineList = ssc.union(kafkaDstreams)
    lineList.persist()
    val lines = lineList.map(x => x._2)
    //将展现日志按adlist拆开
    val searchfenkailines = lines.flatMap(line => line.split("\n")).flatMap(line => {
      val re: Array[String] = adSerchConertToArray(line)
      re
    })
    lineList.foreachRDD(_.unpersist())
    //为每条日志 添加一个key，key的值sid+pos+position
    val sidgrouplines = searchfenkailines.filter(line => line.split("\001").length == 24).map(value => {
      val strarray = value.split("\001")
      (strarray(12) + strarray(20) + strarray(21), value)
    })

    sidgrouplines

  }
}
