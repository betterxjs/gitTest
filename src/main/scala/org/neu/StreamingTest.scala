package org.neu

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jsjie2 on 2019/4/16.
  */
class StreamingTest {
    def main1() {
      val conf = new SparkConf().setMaster("local").setAppName("FileStreaming")
      val sc = new StreamingContext(conf,Seconds(5))

      val lines = sc.textFileStream("d:/a")
      val words = lines.flatMap(_.split(" "))
      val wordCounts = words.map(x => (x , 1)).reduceByKey(_ + _)
      wordCounts.print()
      sc.start()
      sc.awaitTermination()
    }
}
object test extends App{
  val st=new StreamingTest
  st.main1()
  val conf=new SparkConf().setMaster("local[*]").setAppName("FileStreaming")
  val ssc=new StreamingContext(conf,Seconds(3))
  val dstream=ssc.textFileStream("")
  val wd=dstream.window(Seconds(2))
}
