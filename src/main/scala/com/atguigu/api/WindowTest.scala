package com.atguigu.api

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


/**
  * @author xjp
  */

// 开窗函数的使用
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //定义时间语义为事件时间
    env.getConfig.setAutoWatermarkInterval(100) //设置周期型水位线时间间隔

    // 备注：直接读取文件会直接退出；
    //    val inputStream: DataStream[String] = env.readTextFile("H:\\IDEA\\Flink\\flinkTutorial\\src\\source.txt")

    val dataDS: DataStream[String] = env.socketTextStream("warehousehadoop102", 7777)

    val mapTranDS: DataStream[SensorReading] = dataDS.map(
      line => {
        val strings: Array[String] = line.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
      })
      //.assignAscendingTimestamps(data => data.timestamp * 1000L) //升序数据的时间戳提取
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) { //设置最大延迟时间为3s
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L //指定提取对象，提取对象为SensorReading的时间戳
    })

    // 1.滚动窗口(timeWindow算子实际是window(new TumblingEventTimeWindows())
    //   或window(new TumblingProcessingTimeWindows())的缩写，本质等效)
    /*    val aggDS: DataStream[SensorReading] = mapTranDS
          .keyBy(0)
          //      .timeWindow(Time.seconds(20))
          .window(TumblingEventTimeWindows.of(Time.hours(1), Time.minutes(10))) //带10分钟偏移量的1小时滚动窗口
          .minBy("temperature")

        // 2.滑动窗口
        mapTranDS
          .keyBy(0)
          //.timeWindow(Time.seconds(10), Time.seconds(3))   一般使用该方式定义滑动窗口
          .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(3)))   //10秒窗口，

        // 3.会话窗口(使用较少)
        val sessionDS: WindowedStream[SensorReading, Tuple, TimeWindow] = mapTranDS
          .keyBy(0)
          .window(EventTimeSessionWindows.withGap(Time.seconds(10)))*/

    //  开窗聚合操作
    val winDS: WindowedStream[SensorReading, Tuple, TimeWindow] = mapTranDS
      .keyBy(0)
      .timeWindow(Time.seconds(10))
      .allowedLateness(Time.minutes(1)) //设置允许延迟的时间间隔
      .sideOutputLateData(new OutputTag[SensorReading]("late-data")) //对于超过两次延迟时间之和的数据收集到侧输出流中

    val aggDS: DataStream[SensorReading] = winDS.reduce(new MyMaxTemp())

    aggDS.getSideOutput(new OutputTag[SensorReading]("late-data")).print("late")
    mapTranDS.print("data")
    aggDS.print("agg")

    env.execute("window api job")
  }
}


//下面的ReduceFunction和AggrationFunction均为增量聚合函数，另一种函数为全窗口函数
// 自定义取窗口最大温度值的聚合函数
class MyMaxTemp() extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading =
    SensorReading(value1.id, value2.timestamp + 1, value1.temperature.max(value2.temperature))
}

//自定义求平均温度的聚合函数
class MyAvgTemp() extends AggregateFunction[SensorReading, (String, Double, Int), (String, Double)] {
  override def add(value: SensorReading, accumulator: (String, Double, Int)): (String, Double, Int) = {
    (value.id, value.temperature + accumulator._2, accumulator._3 + 1)
  }

  override def createAccumulator(): (String, Double, Int) = ("", 0.0, 0)

  override def getResult(accumulator: (String, Double, Int)): (String, Double) = {
    (accumulator._1, accumulator._2 / accumulator._3)
  }

  override def merge(a: (String, Double, Int), b: (String, Double, Int)): (String, Double, Int) = {
    (a._1, a._2 + b._2, a._3 + b._3)
  } //可以不做处理
}
