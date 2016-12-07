package com.bj58

import com.bj58.javautils.{JsonData, KafkaRealtimeSearchOdsProducer}
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.joda.time.DateTime

import scala.collection.JavaConverters._

/**
  * Created by 58 on 2016/11/16.
  */

object sparkstudyceshi3 {
    def adSerchConertToArray(strline:String):Array[String] ={
      val linestrWlist = strline.split("\001")
      if(linestrWlist.length==24){
        val result:Array[String] =new Array[String](1)
        result(0)=strline
        result
      }else if(linestrWlist.length==26){
        val linestrWlistPre: Array[String] = new Array(25)
        linestrWlist.copyToArray(linestrWlistPre, 0, 25)
        val srtPre:String =linestrWlistPre.mkString("\001");
        val ad: String = linestrWlist(25)
        val adinfolost=JsonData.adSerchStringToJSONArray(ad)
        val result:Array[String] =new Array[String](adinfolost.length)
        var i:Int=0
        for(item<-adinfolost){
          result(i)=srtPre+"\001"+item
          i=i+1
        }
        result
      }else{
        val result:Array[String] =new Array[String](1)
        result(0)="-"
        result
      }
    }

  def main(args: Array[String]): Unit = {
    val (zkQuorum, groupId, topics, numThreads) = (
        "10.126.99.105:2181,10.126.99.196:2181,10.126.81.208:2181,10.126.100.144:2181,10.126.81.215:2181/58_kafka_cluster",
        "hdp_lbg_ectech_lm_ods_click_seaech_ceshi",
        "hdp_lbg_ectech_lm_ods_click,hdp_lbg_ectech_lm_ods_search",
        1)
    val kafkaParams = Map[String, String]("zookeeper.connect" -> "10.126.99.105:2181,10.126.99.196:2181,10.126.81.208:2181,10.126.100.144:2181,10.126.81.215:2181/58_kafka_cluster",
        "group.id" -> "hdp_lbg_ectech_lm_ods_click_seaech_ceshi", "zookeeper.connection.timeout" -> "10000")
    val numInputStream = 4
    val ssc = new StreamingContext(new SparkConf().set("spark.kryoserializer.buffer.max","1024m").setAppName("sparkstudy"), Seconds(360))
    val topicMap = topics.split(",").map((_, numThreads)).toMap
    val kafkaDstreams = (1 to numInputStream).map {
          _ => KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
        }
    val lineList = ssc.union(kafkaDstreams)
        lineList.persist()
    val lines = lineList.map(x => x._2)
    //将展现日志按adlist拆开
    val searchfenkailines=lines.flatMap(line=>line.split("\n")).flatMap(line =>{
          val re:Array[String]=adSerchConertToArray(line)
              re
        })
    lineList.foreachRDD(_.unpersist())
    //为每条日志 添加一个key，key的值sid+pos+position
    val sidgrouplines=searchfenkailines.map(value =>{
          val strarray=value.split("\001")
          if(strarray.length==34){
            (strarray(5)+strarray(32)+strarray(33),value)
          }else if(strarray.length==24){
            (strarray(12)+strarray(20)+strarray(21),value)
          }else{
            ("-","-")
          }
        })

    val midlledetail=sidgrouplines.combineByKey(List(_), (c:List[String],v:String)=>v::c, (c1:List[String],c2:List[String])=>c1:::c2,new HashPartitioner(48),true)


   // midlledetail.reduceByWindow((c1:List[String],c2:List[String])=>c1:::c2,Seconds(120), Seconds(30))
    //使用window
    //val midlledetail=sidgrouplines.reduceByKeyAndWindow((c1:List[String],c2:List[String])=>c1:::c2,Seconds(120), Seconds(30))

    val detaillogs=midlledetail.mapValues(vlaue =>JsonData.adGroupBysidToOne(vlaue.asJava)).map(_._2).filter(line => {
       !line.equals("-")
       })
      detaillogs.persist()

    // 以时间为单位，将日志分组
    val detaillogshandle=detaillogs.map(value =>{
      val strarray=value.split("\001")
      if(strarray.length>=7){
        (strarray(6),value)
      }else{
        ("-",value)
      }
    }).filter( line => !line._1.equals("-"))



    detaillogshandle.foreachRDD(rdditem => {
      if(!rdditem.isEmpty()) {
        val max=rdditem.max()._1
        val min=rdditem.min()._1
        val maxlong=max.toLong
        val minlong=min.toLong
        val middle=minlong+(maxlong-minlong)/10
        val time = new DateTime

        rdditem.saveAsTextFile("/home/hdp_lbg_ectech/middata/dingxiao/spark/ceshi_7/"+time.toString("yyyy_MM_dd_HH_mm_ss")+"_"+min.toString +"_"+max.toString+"_"+middle.toString)
        rdditem.foreachPartition(x => {
          x.foreach(record => {
            val currenttime=record._1.toLong
            if(currenttime>=minlong && currenttime<=middle){
               KafkaRealtimeSearchOdsProducer.producerSend(record._2, "hdp_lbg_ectech_lm_dwd_detail")
            }
          })
        })
      }
    })

    detaillogs.foreachRDD(_.unpersist())
    ssc.start()
    ssc.awaitTermination()
  }
}
