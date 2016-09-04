package com.srt.iis.spark



import kafka.serializer.StringDecoder
import net.sf.json._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
//import com.srt.iis.spark.jni


/**
  * Created by root on 16-7-25.
  */

object carDetect {
//def main(args: Array[String]): Unit = {
//  System.loadLibrary("CarAnalysis")
//  val sdk = new CarAnalysis
//
//  val json = sdk.getCar("/home/test.jpg")
//  println(json)
//    // Create a StreamingContext with the given master URL
//    val conf = new SparkConf().setAppName("UserClickCountStat")
//    val ssc = new StreamingContext(conf, Seconds(2))
//
//    // Kafka configurations
//    //val topics = Set("user_events")
//    val topics = Set("carDetect")
//    val brokers = "10.150.26.241:9092,10.150.26.242:9092,10.150.26.243:9092,10.150.26.244:9092"
//    val kafkaParams = Map[String, String](
//      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")
//
//    // Create a direct stream
//    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//
//    //kafkaStream.for
//    val events = kafkaStream.flatMap(line => {
//       // val data = line._2.split(" ")
//      val data = JSONObject.fromObject(line._2)
//      val dataArray = data.getJSONArray("pic_array").toArray()
//      dataArray
//      //Some(dataArray)
//    })
//
//    //val test = events.flatMap(x => (x.toArray()))
//    //test.print()
//    //val test = events.map(x => (x.getString("pic_array"),1))
//    //val test = events.map(x => (x.getJSONArray("pic_array"),1))
//
//
//    //test.print()
//    //val test = events.flatMap(_.toArray())
//    println("============================step1===========================")
//    // Compute user click times
//    // val userClicks = events.map(x => (x.getString("uid"), x.getInt("click_count"))).reduceByKey(_ + _)
//
//    val userClicks = events.map(x => (JSONObject.fromObject(x).getString("id"), JSONObject.fromObject(x).getString("picUrl"))).reduceByKey(_+_)
//
////    userClicks.foreachRDD(rdd => {
////        rdd.foreachPartition(partitionOfRecords => {
////          partitionOfRecords.foreach(pair => {
////            val uid = pair._1
////            val url = pair._2
////            val result = detect(url)
////            println(result)
////          })
////        })
////    })
//    userClicks.foreachRDD(rdd => {
//        //rdd.map(x=>println(x._1)).collect()
//        rdd.map(x=>detect(x._1)).collect()
//        /*System.loadLibrary("CarAnalysis")
//        val sdk = new CarAnalysis*/
//        /*val json = sdk.getCar("/home/test.jpg")
//        println(json)*/
//     })
//
//    ssc.start()
//    ssc.awaitTermination()
//
//
//    //      rdd.foreachPartition(partitionOfRecords => {
//    //        partitionOfRecords.foreach(pair => {
//    //          val uid = pair._1
//    //          println(uid)
//    //          val clickCount = pair._2
//    //          /*val jedis = RedisClient.pool.getResource
//    //          jedis.select(dbIndex)
//    //          jedis.hincrBy(clickHashKey, uid, clickCount)
//    //          RedisClient.pool.returnResource(jedis)
//    //        })
//    //      })
//    //println(rdd)
//  }

  def detect(url:String):String={
    println("=======test======")
    //println(System.getProperty("java.library.path"))
    System.loadLibrary("CarAnalysis")
    val sdk = new CarAnalysis

    //val json = "aaaa"
    val json = sdk.getCar("/home/test.jpg")
    println(json)
    //println("=======end======")
    json
  }

}

