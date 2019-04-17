package com.iflytek.test

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}

/**
  * Created by jsjie2 on 2019/4/8.
  */

object TF_IdfUtil {
  def tf_idf(tagStrRdd:RDD[Array[String]], sc:SparkContext) = {
    val sqlContext=new SQLContext(sc)
    import sqlContext.implicits._
    val tagRDD=tagStrRdd.map(("hello",_)).toDF("testStr","tags") //指定列名，使用case class可以不指定列名

    tagRDD.show(false)
    //tagStrRdd.map(MusicTag(_)).toDF().show()
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("tags")
      .setOutputCol("tag_tf")
      .fit(tagRDD)

    //tf
    val tag_tf = cvModel.transform(tagRDD)
    tag_tf.show(false)

    //idf
    val idf = new IDF().setInputCol("tag_tf").setOutputCol("tf_idf");
    val idfModel = idf.fit(tag_tf);
    val idfDf = idfModel.transform(tag_tf).drop("tags", "tag_tf")

    //将DataFrame转换成RDD[Row]，之后转换成RDD[SparseVector]
    val idfRDD = idfDf.rdd.map(_.getAs[SparseVector]("tf_idf"))
    //词典数组，数组每一个元素代表词典中一个词
    val tagDict = cvModel.vocabulary
    val bTagDict = sc.broadcast(tagDict)
//    val df= idfDf.map{
//      case Row(str:String, value:SparseVector) =>
//        val dict = bTagDict.value
//        val tagArr = value.indices.map(dict(_))
//        //idf值,取2位小数
//        val tagScoreArr = value.values
//        val tagZipScore = tagArr.zip(tagScoreArr).sorted(new Ordering[(String,Double)] {
//          override def compare(x: (String, Double), y: (String, Double)): Int = y._2.compareTo(x._2)
//        })
//        //.sortBy(_._2).reverse
//        (str,tagZipScore)
//    }
//    //df.show(false)
//    df

    //将每首歌曲的tf-idf值和相应的标签进行关联 =>RDD[Array[(tag,score)]]
    val tagZipScore = idfRDD.map {
      case sparseVector =>
        val dict = bTagDict.value
        val tagArr = sparseVector.indices.map(dict(_))
        //idf值,取2位小数
        val tagScoreArr = sparseVector.values
        val tagZipScore = tagArr.zip(tagScoreArr).sorted(new Ordering[(String,Double)] {
          override def compare(x: (String, Double), y: (String, Double)): Int = y._2.compareTo(x._2)
        })
          //.sortBy(_._2).reverse
        tagZipScore
    }
    tagZipScore
  }
}
