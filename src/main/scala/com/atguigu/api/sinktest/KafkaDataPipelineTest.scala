package com.atguigu.api.sinktest

import java.util.Properties

import com.atguigu.api.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * @author xjp
  */

// 先从kafka中读取数据，处理后再将数据写入kafka中
object KafkaDataPipelineTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 1.从kafka中读取数据
    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "warehousehadoop102:9092,warehousehadoop103:9092")
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group")
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].toString)
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].toString)

    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), prop))
    val dataStream: DataStream[SensorReading] = inputStream
      .map(line => {
        val arr = line.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      })

    // 2.将数据写入kafka

    dataStream.map(data => data.toString)
      .addSink(
        // 由于FlinkKafkaProducer011继承了TwoPhaseCommitSinkFunction，因此可以直接放在此处；
        new FlinkKafkaProducer011[String]("warehousehadoop102:9092", "sinktest", new SimpleStringSchema())
      )

    env.execute("kafka pipeline job")
  }
}
