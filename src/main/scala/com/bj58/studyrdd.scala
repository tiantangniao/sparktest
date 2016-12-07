package com.bj58

import org.apache.spark.{SparkConf, SparkContext}
//import org.joda.time.DateTime

/**
  * Created by 58 on 2016/11/22.
  */
object studyrdd {
  def partitionsFun(/*index : Int,*/iter : Iterator[(String,String)]) : Iterator[String] = {
    var woman = List[String]()
    while (iter.hasNext){
      val next = iter.next()
      next match {
        case (_,"female") => woman = /*"["+index+"]"+*/next._1 :: woman
        case _ =>
      }
    }
    return  woman.iterator
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("mappartitions")
    val sc = new SparkContext(conf)


    val kv2=sc.parallelize(List(("A",4),("A",4),("C",3),("A",4),("B",5)),5)
        val k3=kv2.groupByKey().map(line => {
          val d=line._2
          var c=0
          var dd=0
          d.foreach(value=>{
            c=c+value
            dd=dd+1
          })
          (line._1,c/dd)
        })


    val maxlong="1480148599220".toLong
    val minlong="1480148498650".toLong
    val cou=(maxlong-minlong)/10
    val middle=minlong+(maxlong-minlong)/10
    println(maxlong)
    println(minlong)
    println(cou)
    println(middle)

//    var time = new DateTime
//    var d=time.toString("yyyy-MM-dd HH:mm:ss")
//    //rdd.saveAsTextFile(Constant.DISP_LOG_ODS_PATH + time.get("yyyyMMdd") + "/" + time.getHourOfDay+"/"+System.currentTimeMillis())


  }
}
