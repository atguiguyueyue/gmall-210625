package com.atguigu.handle

import java.text.SimpleDateFormat
import java.util.Date
import java.{lang, util}

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandle {
  /**
    * 进行批次内去重
    * @param filterByRedisDStream
    */
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    //将数据转为K，v格式，为了下面使用groupByKey
    val midAndLogDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(log => {
      ((log.mid, log.logDate), log)
    })
    //对相同mid以及同一天的数进行分组聚合
    val midAndLogDateToIterLogDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndLogDateToLogDStream.groupByKey()

    //按照时间戳由小到大进行排序,并取第一条数据，因为是以用户第一次登录为标准
    val midAndLogDateToListLogDStream: DStream[((String, String), List[StartUpLog])] = midAndLogDateToIterLogDStream.mapValues(log => {
      log.toList.sortWith(_.ts < _.ts).take(1)
    })
    //使用flatMap算子打散List集合中的数据
   val value: DStream[StartUpLog] = midAndLogDateToListLogDStream.flatMap(_._2)

    value
  }

  /**
    * 批次间去重 （首先获取redis中保存的mid->拿当前mid与查询出的mid做对比，如果有重复，则过滤掉）
    *
    * @param startUpLogDStream
    * @return
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext) = {
    /*  val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
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
      */

    //在每个分区下获取连接，以减少连接个数
    /*   val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
         //获取redis连接
         val jedis: Jedis = new Jedis("hadoop102", 6379)
         val logs: Iterator[StartUpLog] = partition.filter(log => {
           val redisKey: String = "DAU:" + log.logDate
           //直接利用redis中的方法，判断数据是否存在
           val boolean: lang.Boolean = jedis.sismember(redisKey, log.mid)
           !boolean
         })
         jedis.close()
         logs
       })
       value*/
    //在每个批次中获取一次连接
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //1.在Driver端获取redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      val redisKey: String = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))
      val mids: util.Set[String] = jedis.smembers(redisKey)

      //将查询出来的mid的数据广播到executor端
      val midsBC: Broadcast[util.Set[String]] = sc.broadcast(mids)
      val filterRDD: RDD[StartUpLog] = rdd.filter(log => {
        !midsBC.value.contains(log.mid)
      })

      jedis.close()
      filterRDD
    })
    value
  }

  /**
    * 将mid保存至Redis
    *
    * @param startUpLogDStream
    */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        //创建redis连接
        val jedis: Jedis = new Jedis("hadoop102", 6379)
        partition.foreach(log => {
          val redisKey: String = "DAU:" + log.logDate
          jedis.sadd(redisKey, log.mid)
        })
        //关闭redis连接
        jedis.close()
      })
    })

  }


}
