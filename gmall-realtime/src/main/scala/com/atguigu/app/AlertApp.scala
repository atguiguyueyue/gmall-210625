package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3.消费Kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    //4.将数据转为样例类（将数据转为K，v形式）
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToEventLogDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        val times: String = sdf.format(new Date(eventLog.ts))

        //补全时间字段
        eventLog.logDate = times.split(" ")(0)
        eventLog.logHour = times.split(" ")(1)

        (eventLog.mid, eventLog)
      })
    })
//    midToEventLogDStream.print()

    //5.开窗
    val windowMidToLogDStream: DStream[(String, EventLog)] = midToEventLogDStream.window(Minutes(5))

    //6.分组聚合(mid)
    val midToIterLogDStream: DStream[(String, Iterable[EventLog])] = windowMidToLogDStream.groupByKey()

    //7.根据条件筛选数据
    /**
      * 三次及以上用不同账号登录并领取优惠劵，（过滤出领取优惠券行为，把相应的uid存放到set集合中，并统计个数）
      * 并且过程中没有浏览商品。（如果用户有浏览商品的行为，则不作分析）
      */
      //返回疑似预警日志
    val boolToCouponAlertDStream: DStream[(Boolean, CouponAlertInfo)] = midToIterLogDStream.mapPartitions(partition => {
      partition.map { case (mid, iter) =>
        //创建用来存放领优惠券的用户id
        val uids: util.HashSet[String] = new util.HashSet[String]()
        //创建用来存放领优惠券所涉及的商品id
        val itemIds: util.HashSet[String] = new util.HashSet[String]()
        //创建用来存放用户所涉及事件的集合
        val events: util.ArrayList[String] = new util.ArrayList[String]()

        //创建一个标志位，用来判断用户是否有浏览商品行为
        var bool: Boolean = true

        breakable {
          iter.foreach(log => {
            //添加用户涉及行为
            events.add(log.evid)
            if ("clickItem".equals(log.evid)) {
              //浏览商品行为
              bool = false
              break()
            } else if ("coupon".equals(log.evid)) {
              //添加用户id
              uids.add(log.uid)
              //添加涉及商品id
              itemIds.add(log.itemid)
            }
          })
        }
        (uids.size() >= 3 & bool, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
    })

    //8.生成预警日志
    val couponAlertInfoDStream: DStream[CouponAlertInfo] = boolToCouponAlertDStream.filter(_._1).map(_._2)

    couponAlertInfoDStream.print()
    //9.将预警日志写入ES并去重
    couponAlertInfoDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val list: List[(String, CouponAlertInfo)] = partition.toList.map(log => {
          (log.mid + log.ts / 1000 / 60, log)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_ALERT_INDEX+"0625",list)
      })
    })

    //10.开启任务并阻塞
    ssc.start()
    ssc.awaitTermination()

  }

}
