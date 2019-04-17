package org.neu

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession

/**
  * Created by jsjie2 on 2019/4/2.
  */
object TfIDFTest extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder.master("local").appName("Simple Application").getOrCreate()

  val wordsData = spark.createDataFrame(Seq(
    (0.0, "你好 I I heard about Spark"),
    (0.0, "I wish Java could use case classes"),
    (1.0, "I Logistic regression models are neat")
  )).toDF("label", "sentence")

  val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
  val wordsData1 = tokenizer.transform(wordsData)

  val cvModel: CountVectorizerModel = new CountVectorizer()
    .setInputCol("words")
    .setOutputCol("feature")
    .fit(wordsData1)

  val cvDf= cvModel.transform(wordsData1)
  cvDf.show(false)

  val idf1 = new IDF().setInputCol("feature").setOutputCol("features");
  val idfModel1 = idf1.fit(cvDf);
  val idfDf = idfModel1.transform(cvDf).drop("words","feature")


  idfDf.show(false);
  val a=idfDf.rdd.take(1)(0).getAs[SparseVector]("features")
  val index_score=a.indices.zip(a.values)
  val dict=cvModel.vocabulary
  val singer_score=index_score.map{
    case(index, score) =>
      val singer= dict(index)
      (singer, score)
  }
  singer_score.foreach(println(_))


 //println(a)

  println(cvModel.vocabulary.length)

//  val df = spark.createDataFrame(Seq(
//    (0, Array("a", "b", "c", "d")),
//    (1, Array("a", "b", "b", "c", "a","d","e"))
//  )).toDF("id", "words")
//
//  // fit a CountVectorizerModel from the corpus
//  val cvModel1: CountVectorizerModel = new CountVectorizer()
//    .setInputCol("words")
//    .setOutputCol("features")
//    .fit(df)
//
//  cvModel1.transform(df).show(false)
}
