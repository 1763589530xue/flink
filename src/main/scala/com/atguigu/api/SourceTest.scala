package com.atguigu.api

import java.util.{Properties, Random}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.immutable

/**
  * @author xjp
  */

// source的使用

object source {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //1.从集合中读取数据，使用fromCollection算子
    val inputStream1: DataStream[SensorReading] = env.fromCollection(
      List(
        SensorReading("sensor_1", 1547718199, 35.8),
        SensorReading("sensor_6", 1547718201, 15.4),
        SensorReading("sensor_7", 1547718202, 6.7),
        SensorReading("sensor_10", 1547718205, 38.1),
        SensorReading("sensor_1", 1547718199, 38.3),
        SensorReading("sensor_1", 1547718199, 35.1),
        SensorReading("sensor_1", 1547718199, 36.2)
      )
    )
    //    inputStream1.print("stream1:").setParallelism(1)

    //2.直接添加元素（使用较少）
    //   env.fromElements(0,0.67,"hello")

    //3.从文件中读取数据
    val inputStream2: DataStream[String] = env.readTextFile("H:\\IDEA\\Flink\\flinkTutorial\\src\\source.txt")
    // inputStream2.print("stream2")

    // 4.从socket中读取数据(使用较少)
    val inputStream3: DataStream[String] = env.socketTextStream("warehousehadoop102", 7777)

    // 5.从kafka中读取数据
    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "warehousehadoop102:9092,warehousehadoop103:9092")
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group")
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].toString)
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].toString)

    // 下行中的SimpleStringSchema为首次使用
    val inputStream4: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), prop))
    //    inputStream4.print()

    // 6.自定义SourceFunction
    val inputStream5: DataStream[SensorReading] = env.addSource(new MySensorSource())
    inputStream5.print()

    env.execute("source test")
  }
}

case class SensorReading(id: String, timestamp: Long, temperature: Double)

class MySensorSource() extends SourceFunction[SensorReading] {
  //  重写方法1：取消执行方法
  var running: Boolean = true

  override def cancel(): Unit = running = false

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]) = {
    val rand = new Random()
    var curTempList: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(
      i => ("senser_" + i, 60 + rand.nextGaussian() * 20)
    )

    // 无限循环生成随机传感器数据
    while (running) {
      curTempList = curTempList.map(
        currTuple => (currTuple._1, currTuple._2 + rand.nextGaussian())
      )

      val curTs: Long = System.currentTimeMillis()

      // 将数据包装成样例类用sourceContext进行输出
      curTempList.map(
        currTuple => sourceContext.collect(SensorReading(currTuple._1, curTs, currTuple._2))
      )

      Thread.sleep(2000)
    }
  }
}