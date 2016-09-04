package com.srt.iis.spark

import java.io._
import java.net.{DatagramPacket, DatagramSocket, Socket}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import com.boyahualu.video.{Factory, VideoAlgorithmProcessor, ExtractRet}
import io.netty.buffer.ByteBufUtil
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.bouncycastle.pqc.math.linearalgebra.ByteUtils

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import com.boyahualu.video

/**
  * Created by root on 16-8-20.
  */
class VideoReceiver(host:String, port:Int)
  extends Receiver[Array[Byte]](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  def onStart(): Unit ={
    new Thread("Video Receiver"){
      override def run(){receive()}
    }.start()
  }

  def onStop(): Unit ={

  }

//  private def receive(): Unit ={
//    var socket: DatagramSocket = null
//    var buf:ArrayBuffer[Byte] = null
//    var tempBuf:Array[Byte] = null
//    var packet:DatagramPacket = null
//    try{
//      socket = new DatagramSocket(port)
//
////      val reader = new BufferedReader(
////        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
//      var pBuf:Array[Byte] = new Array[Byte](1024)
//      packet = new DatagramPacket(pBuf, 1024)
//      socket.receive(packet)
//      tempBuf = packet.getData()
//      //reader.read(tempBuf)
//      //buf ++= tempBuf.map(_.toByte)
//      if(tempBuf.length > 0){
//        buf ++= tempBuf
//      }
//
//      println("=========="+tempBuf.length+"=========")
//      while(!isStopped && tempBuf != null){
//        println("============recevie data===============")
//        var gopStart = findGop(buf, 0)
//        while(gopStart != -1){
//          val gopEnd = findGop(buf, gopStart+5)
//          if(gopEnd != -1){
//            val arr:Array[Byte] = null
//            buf.slice(gopStart, gopEnd).copyToArray(arr)
//            store(ByteBuffer.wrap(arr))
//            buf.remove(gopEnd)
//          }
//          gopStart = findGop(buf, 0)
//        }
////        reader.read(tempBuf)
////        buf ++= tempBuf.map(_.toByte)
//        socket.receive(packet)
//        tempBuf = packet.getData()
//        //reader.read(tempBuf)
//        //buf ++= tempBuf.map(_.toByte)
//        buf ++= tempBuf
//      }
//
//      //reader.close()
//      socket.close()
//      restart("Try to connect again")
//    }catch{
//      case e: java.net.ConnectException =>
//        restart("Error connecting to " + host + ":" + port, e)
//      case t: Throwable =>
//        socket.close()
//        socket.disconnect()
//        restart("Error receiving data", t)
//    }
//  }


  private def receive(): Unit = {
    var socket = new DatagramSocket(6000)
    var buf: ArrayBuffer[Byte] = new ArrayBuffer[Byte]()
    var tempBuf: Array[Byte] = null
    var pBuf: Array[Byte] = new Array[Byte](1024)
    var packet = new DatagramPacket(pBuf, 1024)
    var gopStart: Int = -1
    var gopEnd: Int = -1
    var count:Int = 0

    try{
      socket.receive(packet)
      tempBuf = packet.getData()
      buf ++= tempBuf

      while (tempBuf != null) {
        //logInfo("============recevie data===============")
        //println("length is "+ buf.length)
        if (gopStart == -1) {
          gopStart = findGop(buf, 0)
        }
        //println("gopStart is "+ gopStart)

        if (gopStart != -1) {
          if (gopEnd == -1) {
            gopEnd = findGop(buf, gopStart + 5)
          }
          else {
            gopEnd = findGop(buf, gopEnd - 5)
          }

          if (gopEnd != -1) {
            count += 1
            logInfo("===== got a gop======")
            if (count == 1) {
              val arr: Array[Byte] = new Array[Byte](gopEnd - gopStart + 1)
              buf.slice(gopStart, gopEnd).copyToArray(arr)
              store(arr)

              count = 0
              buf.remove(0, gopEnd)
              logInfo("===== got a slice======")

              gopStart = -1
              gopEnd = -1
            }


            gopEnd = buf.length

          } else {
            //
            gopEnd = buf.length
          }
        }

        socket.receive(packet)
        tempBuf = packet.getData()
        buf ++= tempBuf
      }
    }catch{
      case e: java.net.ConnectException =>
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        socket.close()
        socket.disconnect()
        restart("Error receiving data", t)
    }
  }


  private def findGop(buf:ArrayBuffer[Byte], index:Int):Int={
    var pos = -1
    var start = buf.indexOf(0, index)
    //println("header is "+ buf(start) + " " + buf(start+1) + " " + buf(start+2) + " " + buf(start+3) + " " + buf(start+4))
    println(start)
    breakable{
      while(start != -1 && start+4 < buf.length){
        if(buf(start+1) == 0 && buf(start+2) == 0 && buf(start+3) == 1 && buf(start+4)%16 == 7){
          logInfo("======================find head!!")
          logInfo("======================start pos is " + start)
          //println("tag is " + buf(start+4))
          pos = start
          break()
        }
        start = buf.indexOf(0, start+1)
        //println("new start pos:" + start)
      }
    }
    pos
  }

}
