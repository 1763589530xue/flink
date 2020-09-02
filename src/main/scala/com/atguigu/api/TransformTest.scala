package com.atguigu.api

import org.apache.flink.api.common.functions.{FilterFunction, RichFlatMapFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @author xjp
  */

// flink中transfrom过程的测试

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.readTextFile("H:\\IDEA\\Flink\\flinkTutorial\\src\\source.txt")
    // 1.基本转换算子
    val mapTranDS: DataStream[SensorReading] = inputStream.map(
      line => {
        val strings: Array[String] = line.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
      })
    // .filter(new MyFilter("sensor_1")) //此行使用了FilterFunction函数，优点为通用性强
    //    mapTranDS.print()

    // 2.分组聚合
    // ①简单滚动聚合，求每个传感器所有温度的最小值
    val aggStream: DataStream[SensorReading] = mapTranDS.keyBy("id").minBy("temperature")
    //    aggStream.print()

    //② 一般化聚合，输出(id,最新时间戳+1，最小温度值)
    val reduseStream: DataStream[SensorReading] = mapTranDS.keyBy("id").reduce((curState, newData) => {
      SensorReading(newData.id, newData.timestamp + 1, curState.temperature.min(newData.temperature))
    })
    //    reduseStream.print()

    // 3.多流转换
    // ①分流（需要split和collet联合使用）
    val splitDS: SplitStream[SensorReading] = mapTranDS.split(
      data => if (data.temperature > 30) List("high") else List("low")
    )

    val highTempDS: DataStream[SensorReading] = splitDS.select("high")
    val lowTempDS: DataStream[SensorReading] = splitDS.select("low")
    val allTempDS = splitDS.select("high", "low")

    //    highTempDS.print("high")
    //    lowTempDS.print("low")
    //    allTempDS.print("all")

    // ②连接两条流(类似"一国两制"，需要connect和map联合使用)
    val warningDS: DataStream[(String, Double)] = highTempDS.map(data => (data.id, data.temperature))
    val connectDS: ConnectedStreams[(String, Double), SensorReading] = warningDS.connect(lowTempDS)
    //两个DS的输出格式可以不同
    val resultDS: DataStream[Product with Serializable] = connectDS.map(
      warningData => (warningData._1, warningData._2, "high temp warning"),
      lowTemptData => (lowTemptData.id, "low temp")
    )
    //    resultDS.print()

    //③连接类型相同的多条流
    val unionDS: DataStream[SensorReading] = highTempDS.union(lowTempDS, allTempDS)
    unionDS.print()

    env.execute("transform test")
  }

}

// 自定义函数类
class MyFilter(word: String) extends FilterFunction[SensorReading] {
  override def filter(value: SensorReading): Boolean = value.id.startsWith(word)
}

// 富函数(实现的功能更加丰富，并且方法有生命周期)
class MyRichFlatMapper extends RichFlatMapFunction[SensorReading, String] {

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def flatMap(value: SensorReading, out: Collector[String]): Unit = ???

  override def close(): Unit = super.close()

}