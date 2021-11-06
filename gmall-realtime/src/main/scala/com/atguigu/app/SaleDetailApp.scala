package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import collection.JavaConverters._

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //3.分别消费kafka中订单表的数据以及订单明细表的数据
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    //4.分别将两个流的数据转为样例类
    val orderInfoDStream = orderInfoKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //补全字段
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        (orderInfo.id, orderInfo)
      })
    })

    val orderDetailDStream = orderDetailKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })
    })

    //5.双流join
    //    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoDStream.join(orderDetailDStream)
    val fullOutJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)

    //6.采用加缓存的方式处理因网络延迟所带来的数据丢失问题
    val noUserSaleDetail: DStream[SaleDetail] = fullOutJoinDStream.mapPartitions(partition => {
      implicit val formats = org.json4s.DefaultFormats

      //创建List集合用来存放结果数据（SaleDetail）
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()

      //创建redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      partition.foreach { case (orderId, (infoOpt, detailOpt)) =>
        //OrderInfo redisKey
        val orderInfoRedisKey: String = "OrderInfo" + orderId
        val orderDetailRedisKey: String = "OrderDetail" + orderId

        //a.判断订单表的数据是否存在
        if (infoOpt.isDefined) {
          //订单表数据存在
          //a.1获取订单表数据
          val orderInfo: OrderInfo = infoOpt.get
          //a.2判断订单明细表数据是否存在
          if (detailOpt.isDefined) {
            //a.3订单明细表存在，则取出数据
            val orderDetail: OrderDetail = detailOpt.get
            //a.4将两个表的数据组合成样例类
            val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
            details.add(detail)
          }

          //b.将OrderInfo数据存入Redis
          //b.1将样例类转为JSON字符串
          val orderInfoJson: String = Serialization.write(orderInfo)
          //          val str: String = JSON.toJSONString(orderInfo)
          jedis.set(orderInfoRedisKey, orderInfoJson)
          //b.2给存入redis中的数据设置过期时间
          jedis.expire(orderInfoRedisKey, 30)

          //c.查询OrderDetail数据
          //先判断OrderDetailRedisKey是否存在
          if (jedis.exists(orderDetailRedisKey)) {
            //orderDetail数据存在
            val detailSet: util.Set[String] = jedis.smembers(orderDetailRedisKey)
            for (elem <- detailSet.asScala) {
              //将查询出来的JSON串类型的数据转为样例类
              val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
              val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(detail)
            }
          }
        } else {
          //orderInfo数据不存在
          //d.判断OrderDetail数据是否存在
          if (detailOpt.isDefined) {
            //取出OrderDetail数据
            val orderDetail: OrderDetail = detailOpt.get
            //e.查询orderInfo缓存中是否有能Join上的数据
            if (jedis.exists(orderInfoRedisKey)) {
              //有能有与orderDetail join上的数据
              val infoStr: String = jedis.get(orderInfoRedisKey)
              //将查询出来的字符串转为样例类
              val orderInfo: OrderInfo = JSON.parseObject(infoStr, classOf[OrderInfo])
              val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(detail)
            } else {
              //对方缓存中没有能join上的数据
              //f.将自己存入缓存，等待OrderInfo数据到来
              val orderDetailJsonStr: String = Serialization.write(orderDetail)
              jedis.sadd(orderDetailRedisKey, orderDetailJsonStr)
              //对orderDetail数据设置过期时间
              jedis.expire(orderDetailRedisKey, 30)
            }
          }
        }
      }
      jedis.close()
      details.asScala.toIterator
    })

    //7.关联UserInfo数据
    val SaleDetailDStream: DStream[SaleDetail] = noUserSaleDetail.mapPartitions(partition => {
      //创建Redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val details: Iterator[SaleDetail] = partition.map(saleDetail => {
        //查询redis中UserInfo数据
        val userInfoRedisKey: String = "UserInfo:" + saleDetail.user_id
        val userInfoJSONStr: String = jedis.get(userInfoRedisKey)

        //将查询出来的Json字符串转为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJSONStr, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })
      jedis.close()
      details
    })
    SaleDetailDStream.print()

    //8.将数据写入ES
    SaleDetailDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val list: List[(String, SaleDetail)] = partition.toList.map(saleDetail => {
          (saleDetail.order_detail_id, saleDetail)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_SALE_DETAIL_INDEX+"0625",list)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
