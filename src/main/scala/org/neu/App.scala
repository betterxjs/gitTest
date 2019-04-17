package org.neu
import java.math.RoundingMode
import java.text.DecimalFormat

import com.iflytek.test.TF_IdfUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
 * Hello world!
 *
 */
object App {
  case class MusicTag(tag:Array[String])

  def main(args:Array[String]): Unit = {
   // println(this.getClass.getName)
    val a = new DecimalFormat("0.0000")
    a.setRoundingMode(RoundingMode.FLOOR)
    val num= a.format(3.1415926)
    println(num.toDouble)

    println("3.15".toDouble)


        Logger.getLogger("org").setLevel(Level.ERROR)
        val date= args(0)
        val spark = SparkSession.builder.master("local[2]").appName(this.getClass.getName).getOrCreate()
        val sc= spark.sparkContext
        val pathOfPopSingerMusic= s"/user/iflyrd/work/jsjie2/popularMusic/musicOfPopularSingerInMigu/${date}"
        val pathOfPopAndSimSingerMusic= s"/user/iflyrd/work/jsjie2/popularMusic/musicOfPopularAndSimilarSingerInMigu/${date}"
        val pathOfMiguPopMusic= s"/user/iflyrd/work/jsjie2/popularMusic/popularMusicInMiguAndNets/${date}"
        val testPath=s"file:///C:/Users/Administrator/Desktop/test.txt"
        //    val musicOfPopSinger= getMusicFromHdfs(pathOfPopSingerMusic, sc)
        //    val musicOfPopAndSimSinger= getMusicFromHdfs(pathOfPopAndSimSingerMusic, sc)
        //    val musicOfMiguPop=getMusicFromHdfs(pathOfPopAndSimSingerMusic, sc)
        //
        //    val popMusic= musicOfPopSinger.union(musicOfPopAndSimSinger)
        //      .union(musicOfMiguPop)
        //      .distinct()
        //    popMusic.cache()

        val popMusicTest=getMusicFromHdfs(testPath,sc)

        val sqlContext=spark.sqlContext
        import sqlContext.implicits._
        //转换成DataFrame
        val popMusicTag= popMusicTest.map{
          case(itemId, songName, singer, tag) => tag.split(",")
        }
    Int
    val tf_idf=TF_IdfUtil.tf_idf(popMusicTag,sc).map(_.mkString(",")).repartition(3)
    val strTest=sc.textFile("file:///C:/Users/Administrator/Desktop/test1.txt").take(1)(0)
    val array=strTest.split(",")
    //val random=new Random
    val b:Int=1
    tf_idf.map{
      case str=>
        str+ array(0)
    }.foreach(println(_))
//
//        val cvModel: CountVectorizerModel = new CountVectorizer()
//          .setInputCol("tags")
//          .setOutputCol("tag_tf")
//          .fit(popMusicTag)
//
//        val tag_tf= cvModel.transform(popMusicTag)
//        tag_tf.show(false)
//
//        val idf = new IDF().setInputCol("tag_tf").setOutputCol("tf_idf");
//        val idfModel = idf.fit(tag_tf);
//        val idfDf = idfModel.transform(tag_tf).drop("tags","tag_tf")
//        idfDf.show(false)
//
//        //将DataFrame转换成RDD[Row]，之后转换成RDD[SparseVector]
//        val idfRDD= idfDf.rdd.map(_.getAs[SparseVector]("tf_idf"))
//        //词典数组，数组每一个元素代表词典中一个词
//        val tagDict= cvModel.vocabulary
//        val bTagDict =sc.broadcast(tagDict)
//
//        val tagWithScore= idfRDD.map{
//          case sparseVector =>
//            val dict=bTagDict.value
//            val tagIndexArr= sparseVector.indices.map(dict(_))
//            val tagScoreArr= sparseVector.values
//            val tagWithScore= tagIndexArr.zip(tagScoreArr).map{
//              case (tag, score) => Array(tag, score).mkString(":")
//            }.mkString(",")
//            tagWithScore
//        }
//
//        val itemProfileWithScore= popMusicTest.zip(tagWithScore).map{
//          case ((itemId, songName, singer, tag),tagWithScore) =>
//            (itemId, songName, singer, tagWithScore)
//        }

        //itemProfileWithScore.foreach(println(_))
      }

      def getMusicFromHdfs(path:String , sc:SparkContext):RDD[(String,String,String,String)]={
        val music= sc.textFile(path).map(_.split("~"))
          .filter(_.length==4)
          .map{
            case Array(itemId, songName, singer, tag) =>
              (itemId, songName, singer, tag)
          }
        music

  }

}
