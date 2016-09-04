package org.shirdrn.spark.streaming.utils

import java.io.{FileOutputStream, PrintWriter, File}
import java.lang.Exception
import java.net.{DatagramSocket, DatagramPacket}
import java.nio.ByteBuffer
import java.util.Properties
import com.boyahualu.video.{Factory, ExtractRet, SearchRet, VideoAlgorithmProcessor}
import net.sf.json.JSONArray

import scala.collection.mutable.ArrayBuffer
import scala.util.Properties
import org.codehaus.jettison.json.JSONObject
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import scala.util.Random
import scala.util.control.Breaks._

object KafkaEventProducer {
 
  private val users = Array(
      "4A4D769EB9679C054DE81B973ED5D768", "8dfeb5aaafc027d89349ac9a20b3930f",
      "011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf",
      "068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
      "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
      "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d")
     
  private val random = new Random()
     
  private var pointer = -1
 
  def getUserID() : String = {
       pointer = pointer + 1
    if(pointer >= users.length) {
      pointer = 0
      users(pointer)
    } else {
      users(pointer)
    }
  }
 
  def click() : Double = {
    random.nextInt(10)
  }
 
  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --create --topic user_events --replication-factor 2 --partitions 2
  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --list
  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --describe user_events
  // bin/kafka-console-consumer.sh --zookeeper zk1:2181,zk2:2181,zk3:22181/kafka --topic test_json_basis_event --from-beginning
//  def main(args: Array[String]): Unit = {
//    val topic = "user_events"
//    val brokers = "10.150.26.241:9092,10.150.26.242:9092,10.150.26.243:9092,10.150.26.244:9092"
//    val props = new Properties()
//    props.put("metadata.broker.list", brokers)
//    props.put("serializer.class", "kafka.serializer.StringEncoder")
//
//    val kafkaConfig = new ProducerConfig(props)
//    val producer = new Producer[String, String](kafkaConfig)
//
//    while(true) {
//      // prepare event data
//      val event = new JSONObject()
//      event
//        .put("uid", getUserID)
//        .put("event_time", System.currentTimeMillis.toString)
//        .put("os_type", "Android")
//        .put("click_count", click)
//
//      val event2 = new JSONObject()
//      event
//        .put("uid", getUserID)
//        .put("event_time", System.currentTimeMillis.toString)
//        .put("os_type", "Android")
//        .put("click_count", click)
//
//      var array = new JSONArray()
//      array.add(event)
//      array.add(event2)
//      // produce event message
//      producer.send(new KeyedMessage[String, String](topic, array.toString))
//      println("Message sent: " + event)
//
//      Thread.sleep(200)
//    }
//  }
  private def findGop(buf:ArrayBuffer[Byte], index:Int):Int={
    var pos = -1
    var start = buf.indexOf(0, index)
    //println(buf)
    //println(start)
    breakable{
      while(start != -1 && start+4 < buf.length){
        if(buf(start+1) == 0 && buf(start+2) == 0 && buf(start+3) == 1 && buf(start+4)%16 == 7){
          println("find head!!")
          println("start pos is " + start)
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

  def main(args: Array[String]): Unit = {
    var socket = new DatagramSocket(6000)
    var buf:ArrayBuffer[Byte] = new ArrayBuffer[Byte]()
    var tempBuf:Array[Byte] = null
    var pBuf:Array[Byte] = new Array[Byte](1024)
    var packet = new DatagramPacket(pBuf, 1024)
    var gopStart:Int = -1
    var gopEnd:Int = -1
    var lens:Int = -1
    var ret:ExtractRet = null
    var count:Int = 0

    var vf:VideoAlgorithmProcessor = Factory.CreateProcessor()
    //val writer = new PrintWriter(new File("test.txt"))
    var file:File = new File("/home/test.264")
    var stream:FileOutputStream = new FileOutputStream(file)


    socket.receive(packet)
    tempBuf = packet.getData()
    buf ++= tempBuf


    while(tempBuf != null){
      if(gopStart == -1){
        gopStart = findGop(buf, 0)
      }

      if(gopStart != -1){
        if(gopEnd == -1){
          gopEnd = findGop(buf, gopStart + 5)
        }
        else{
          gopEnd = findGop(buf, gopEnd - 5)
        }

        if(gopEnd != -1){   //
          count+=1
          if(count == 11){
            val arr: Array[Byte] = new Array[Byte](gopEnd - gopStart + 1)
            buf.slice(gopStart, gopEnd).copyToArray(arr)
            stream.write(arr)

            println("=========================================Analysis start")
            println(arr.length)
            ret = vf.ExtractVideo(arr, "test", "H264")
            println(ret.tlist)
            if(ret != null && ret.tlist != null && ret.tlist.length > 0){
              ret.tlist.map( x => {
                println("==== key is :"+ x.key)
                println("==== carPlate is: " + x.carPlate)
                println("==== carType is: " + x.cartype)
              })
            }
            println("Analysis end")
            count = 0
            buf.remove(0, gopEnd)
            println("===== got a slice======")

            gopStart = -1
            gopEnd = -1
          }


          gopEnd = buf.length

        }else{    //
          gopEnd = buf.length
        }

      }

      socket.receive(packet)
      tempBuf = packet.getData()
      buf ++= tempBuf
    }

  }
}