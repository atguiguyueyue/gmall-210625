package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handle.DauHandle
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //3.消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //4.将数据转为样例类，补全字段
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        //补全两个字段 2021-10-30 15
        val times: String = sdf.format(new Date(startUpLog.ts))
        //LogDate
        startUpLog.logDate = times.split(" ")(0)
        //LogHour
        startUpLog.logHour = times.split(" ")(1)

        startUpLog
      })
    })

    //5.做批次间去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandle.filterByRedis(startUpLogDStream)

    //优化：对流做缓存，因为后面多次使用
    startUpLogDStream.cache()
    filterByRedisDStream.cache()

    //打印原始数据的条数
    startUpLogDStream.count().print()

    //打印经过批次间去重后的数据条数
    filterByRedisDStream.count().print()



    //6.做批次内去重

    //7.将去重后的结果（mid）保存在redis中
    DauHandle.saveToRedis(filterByRedisDStream)

    //8.将去重后的结果（明细数据）保存在hbase中



//    //4.打印kafka数据
//    kafkaDStream.foreachRDD(rdd => {
//      rdd.foreach(record => {
//        println(record.value())
//      })
//    }
//    )

    //开启任务并阻塞
    ssc.start()
    ssc.awaitTermination()

  }

}
