package com.atguigu.app

import com.alibaba.fastjson
import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis



object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserInfoApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //3.分别消费kafka中订单表的数据以及订单明细表的数据
    val userInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)

    //4.将用户表的数据转为样例类
    val userInfoDStream = userInfoKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val jSONObject = JSON.parseObject(record.value(),classOf[UserInfo])
//        val value = jSONObject.getString("id")
//        "UserInfo:" + value

        jSONObject
      })
    })
    userInfoDStream.print()

    //5.将userInfo数据写入Redis中
        userInfoKafkaDStream.foreachRDD(rdd=>{
          rdd.foreachPartition(partition=>{
            //创建redis连接
            val jedis: Jedis = new Jedis("hadoop102",6379)
            partition.foreach(record=>{
              val userInfo = JSON.parseObject(record.value(),classOf[UserInfo])
              //userInfo redisKey
              val userInfoRediskey: String = "UserInfo:"+userInfo.id
              jedis.set(userInfoRediskey,record.value())
            })
            jedis.close()
          })
        })

    ssc.start()
    ssc.awaitTermination()
  }

}
