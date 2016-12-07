package com.bj58

import java.io.PrintWriter
import java.net.ServerSocket

import scala.io.Source

object sparkserver {
  def index(n: Int) = scala.util.Random.nextInt(n)

  def main(args: Array[String]) {
    // 获取指定文件总的行数
    val filename = "D:\\dd.txt"
    val lines = Source.fromFile(filename).getLines.toList
    val filerow = lines.length
    // 指定监听某端口，当外部程序请求时建立连接
    val listener = new ServerSocket(9999)

    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)
          var c=0
          while (true) {
            Thread.sleep(1000)
            if(c>991){
              c=0
            }
            // 当该端口接受请求时，随机获取某行数据发送给对方
            val content = lines(c)
            println("-------------------------------------------")
            println(s"Time: ${System.currentTimeMillis()}")
            println("-------------------------------------------")
            println(content)
            out.write(content + '\n')
            out.flush()
            c=c+1
          }
          socket.close()
        }
      }.start()
    }
  }

}
