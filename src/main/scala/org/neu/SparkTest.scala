package org.neu

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

/**
  * Created by jsjie2 on 2019/3/11.
  */
object SparkTest extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf=new SparkConf().setMaster("local").setAppName("")

  val sc=new SparkContext(conf)

  val original_action_rdd=sc.textFile("C:\\Users\\Administrator\\Desktop\\任务\\falco_data\\actions.txt")
    .map(record=>record.split("~"))
    .cache()

//  println(s"total number:${region_action_rdd.count()}")

  val region_channel_rdd=sc.textFile("C:\\Users\\Administrator\\Desktop\\任务\\falco_data\\document.txt")
    .map(record=>record.split("~"))
    .cache()

  val viewData=original_action_rdd.filter(record=>{
    val user_action=record(2)
    user_action=="0"
  }).cache()

  val clickData=original_action_rdd.filter(record=>{
    val user_action=record(2)
    user_action=="1"
  }).cache()
//  println("clickdata:"+clickData.count())

  println("************ view pv *************")
  val view_pv=viewData.count()

  println(s"view_pv:$view_pv")
  println("************ click pv *************")
  val click_pv=clickData.count()

  println(s"click_pv:$click_pv")
  println("************ view uv *************")

  val view_uv=viewData
    .map(record=>{
      val uid=record(0)
      uid
    }).distinct().count()

  println(s"view_uv:$view_uv")
  println("************ click uv *************")

  val click_uv=clickData
    .map(record=>{
      val uid=record(0)
      uid
    }).distinct().count()


  println(s"click_uv:$click_uv")
  println("**************全量去重点击率**************")

  //点击的去重数据 / 浏览的去重数据

  val total_distinct_item_rdd=original_action_rdd.map(record=>{  //点击数据
  val (uid, itemid)=(record(0), record(1))
    (itemid, uid)
  }).distinct.cache()

  val distinct_click_data=clickData.map(record=>{ //全量去重点击数据
    val (uid,itemid)=(record(0),record(1))
    (itemid,uid)
  }).distinct.cache()

  val distinct_click_num=distinct_click_data.count //全量去重点击次数

  val recommend_num=total_distinct_item_rdd.count() //全量去重推荐次数

  val total_click_rate=distinct_click_num.toDouble / recommend_num.toDouble

  println(s"total_click_rate:$total_click_rate")
  println("***********不同频道的全量去重点击率***********")
  //怎么计算推荐次数和点击次数
  val itemid_channel_rdd=region_channel_rdd.map(record=>{
    val (itemid,channel)=(record(0),record(1))
    (itemid,channel)
  })


  //不同频道的推荐次数
  val channel_rc_rdd=total_distinct_item_rdd.join(itemid_channel_rdd).map{
    case (itemid,(uid,channel))=>(channel,1)
  }.reduceByKey(_+_)

  //不同频道的去重点击次数
  val channel_click_rdd=distinct_click_data//没有的频道，说明点击次数为0，使用点击数据关联不上对应channel
   .join(itemid_channel_rdd).map{
    case(itemid,(uid,channel)) => (channel,1)
  }.reduceByKey(_+_)

  //不同频道的全量去重点击率
  val channel_click_rate_rdd=channel_click_rdd.rightOuterJoin(channel_rc_rdd).map{ //
    case(channel,(click_num,rc_num))=>{
      val clicked=click_num.getOrElse(0) //设置点击数默认为0
      val channel_click_rate=clicked.toDouble / rc_num.toDouble
      (channel,channel_click_rate)
    }
  }.sortBy(_._2,false)


  channel_click_rate_rdd.take(100).foreach(println)
  //TODO
  println("***********点击率Top10物品***********")
  //每个物品的推荐次数 使用全量去重数据
  val item_rc_num_rdd=total_distinct_item_rdd.map{
    case(itemid, uid) =>(itemid, 1)
  }.reduceByKey(_+_)

  //每个物品的点击次数 使用去重点击数据
  val item_click_num_rdd=distinct_click_data.map{
    case (itemid,uid)=>(itemid,1)
  }.reduceByKey(_+_)

  //每个物品的去重点击率
  val item_click_rate_rdd=item_click_num_rdd.rightOuterJoin(item_rc_num_rdd)
    .map{
      case(itemid,(clicked,recommend))=>{
        val item_click_num=clicked.getOrElse(0)
        val item_click_rate=item_click_num.toDouble / recommend
        (itemid,item_click_rate)
      }
    }.sortBy(_._2,false)

  item_click_rate_rdd.take(10).foreach(println)

  println("***********用户在频道下的兴趣画像***********")
  //每个用户点击的总数目
  val user_click_num_rdd=clickData.map(record=>{
    val uid=record(0)
    (uid,1)
  }).reduceByKey(_+_)

  //每个用户点击的频道列表
  val user_click_channel_list=clickData.map(record=>{
    val (itemid,uid)=(record(1),record(0))
    (itemid,uid)
  }).join(itemid_channel_rdd)
    .map{
      case(itemid,(uid,channel))=>(uid,channel)
    }.groupBy(tuple=>tuple).map{
    case((uid,channel),iterator)=>{
      val list=iterator.toList
      (uid,(list(0)._2,list.length))
    }
  }.groupBy(_._1).map{
    case(uid,iterable)=>(uid,iterable.map(_._2))
  }

  //用户点击某频道的次数/用户点击的总频道次数
  val user_channel_rate=user_click_num_rdd.join(user_click_channel_list).map{
    case(uid,(user_click_sum,iterator))=>{
      val iter=iterator.map{
        case(channel,click_num)=>{
          (channel,click_num.toDouble / user_click_sum.toDouble)
        }
      }
      (uid,iter)
    }
  }

  user_channel_rate.foreach(println)
  Stream

  sc.stop()
}
