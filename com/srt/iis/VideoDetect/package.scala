/**
  * Created by root on 16-8-20.
  */
package com.srt.iis.spark


import java.io.{FileOutputStream, File}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.Logging
import com.boyahualu.video.{Factory, ExtractRet, SearchRet, VideoAlgorithmProcessor}

object VideoDetect extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("VideoDetectTest")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.receiverStream(new VideoReceiver("10.150.26.220", 6000))

    val data = lines.map(x=> Analysis(x))
    val result = data.map(x => (1, x.length())).reduceByKey(_+_)
    result.print()
//    val words = lines.flatMap(_.split(" "))
//    val wordcount = lines.map(x => (x, x.length())).reduceByKey(_+_)
//    wordcount.print()

    ssc.start()
    ssc.awaitTermination()
  }

  private def Analysis(data:Array[Byte]): String ={
    var vf:VideoAlgorithmProcessor = Factory.CreateProcessor()
    var ret: ExtractRet = null

    var file:File = new File("/home/test.264")
    var stream:FileOutputStream = new FileOutputStream(file)
    stream.write(data)

    logInfo("========Analysis start, data length is " + data.length)
    ret = vf.ExtractVideo(data, "test", "H264")
    println(ret.tlist)
    if(ret != null && ret.tlist != null && ret.tlist.length > 0){
      ret.tlist.map( x => {
        logInfo("==== key is :"+ x.key)
        logInfo("==== carPlate is: " + x.carPlate)
        logInfo("==== carType is: " + x.cartype)
      })
      logInfo("Analysis end")

      ret.tlist(0).key
    }else{
      "no result"
    }

  }
}
