package com.bj58

import collection.JavaConverters._
import com.bj58.javautils.JsonData
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, storage}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark._
/**
  * Created by 58 on 2016/11/16.
  */
object ceshi {
  def adSerchConertToArray(strline:String):Array[String] ={
    val linestrWlist = strline.split("\001")
    if(linestrWlist.length==24){
      val result:Array[String] =new Array[String](1)
      result(0)=strline
      result
    }else{
      val linestrWlistPre: Array[String] = new Array(25)
      linestrWlist.copyToArray(linestrWlistPre, 0, 25)
      val srtPre:String =linestrWlistPre.mkString("\001")
      val ad: String = linestrWlist(25)
      val adinfolost=JsonData.adSerchStringToJSONArray(ad)
      val result:Array[String] =new Array[String](adinfolost.length)
      var i:Int=0
      for(item<-adinfolost){
        result(i)=srtPre+"\001"+item
        i=i+1
      }
      result
    }
  }
  def main(args: Array[String]): Unit = {
          val conf= new SparkConf().setMaster("local").setAppName("sparkstudy")
          val sc= new SparkContext(conf)
          val lines = sc.textFile("D:\\cli1\\luna-adclk.log1.2016-11-16-03,D:\\cli1\\luna-adclk.log2.2016-11-16-03,D:\\cli1\\luna-adclk.log3.2016-11-16-03,D:\\cli1\\luna-adclk.log4.2016-11-16-03,D:\\cli1\\lm-as.log.2016-11-16-03_5")
         //*****************************将展现展开*******************************
          val searchfenkailines=lines.flatMap(line =>{
            val re:Array[String]=adSerchConertToArray(line)
            re
          })
         //******************************以sid,pos,slot********************
          val sidgrouplines=searchfenkailines.map(value =>{
            val strarray=value.split("\001")
            if(strarray.length==34){
              (strarray(5)+strarray(32)+strarray(33),"**"+value)
            }else if(strarray.length==24){
              (strarray(12)+strarray(20)+strarray(21),"##"+value)
            }else{
              ("-","-")
            }
          })


          val midlledetail=sidgrouplines.combineByKey(List(_), (c:List[String],v:String)=>v::c, (c1:List[String],c2:List[String])=>c1:::c2,new HashPartitioner(48),true)


          val detaillogs=midlledetail.mapValues(vlaue =>JsonData.adGroupBysidToOne(vlaue.asJava)).map(_._2).filter(line => {
            !line.equals("-")
          })


          val detaillogshandle=detaillogs.map(value =>{
            val strarray=value.split("\001")
            if(strarray.length>=7){
              (strarray(6),value)
            }else{
              ("-",value)
            }
          })

          val d=detaillogshandle.max()._1
          val d1=detaillogshandle.min()._1









  }
}
