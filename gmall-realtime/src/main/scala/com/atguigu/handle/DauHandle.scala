package com.atguigu.handle

import java.util

import com.atguigu.bean.StartUpLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandle {
  /**
    * 批次间去重 （首先获取redis中保存的mid->拿当前mid与查询出的mid做对比，如果有重复，则过滤掉）
    * @param startUpLogDStream
    * @return
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog]) = {
    val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
      //1.创建redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      //2.查询redis中的数据
      val redisKey: String = "DAU:" + log.logDate
      val mids: util.Set[String] = jedis.smembers(redisKey)

      //3.拿当前的mid与查询出的mid做对比进行过滤
      val bool: Boolean = mids.contains(log.mid)

      //关闭连接
      jedis.close()
      !bool
    })
    value
  }

  /**
    * 将mid保存至Redis
    *
    * @param startUpLogDStream
    */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        //创建redis连接
        val jedis: Jedis = new Jedis("hadoop102",6379)
        partition.foreach(log=>{
          val redisKey: String = "DAU:"+log.logDate
          jedis.sadd(redisKey,log.mid)
        })
        //关闭redis连接
        jedis.close()
      })
    })

  }


}
