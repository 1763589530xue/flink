package com.atguigu.api.sinktest

import com.atguigu.api.{MyRichFlatMapper, SensorReading}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * @author xjp
  */

//将输出存储在redis上
object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(1)

    val inputStream: DataStream[String] = env.readTextFile("H:\\IDEA\\Flink\\flinkTutorial\\src\\source.txt")

    val mapTranDS: DataStream[SensorReading] = inputStream.map(
      line => {
        val strings: Array[String] = line.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
      })

    // 定义jedis连接
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("warehousehadoop102")
      .setPort(6379)
      .build()


    mapTranDS.addSink(new RedisSink[SensorReading](config, new MyRedisMapper()))

    env.execute("redis sink job")
  }
}

//不太懂？？？
class MyRedisMapper() extends RedisMapper[SensorReading] {
  // 写入redis的命令，保存成Hbase表 hset数据结构(hash形式)
  override def getCommandDescription: RedisCommandDescription =
    new RedisCommandDescription(RedisCommand.HSET, "sensor")

  override def getValueFromData(t: SensorReading): String = t.temperature.toString

  override def getKeyFromData(t: SensorReading): String = t.id
}
