package com.atguigu.api

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @author xjp
  */

// flink特有的状态编程
object StateTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 知识点1：选择状态后端(除以下两种还有MemoeryStateBackend)
    //env.setStateBackend(new FsStateBackend("")) //FsStateBackend构造器应该设置存储路径，不设置则按照配置文件路径；
    //env.setStateBackend(new RocksDBStateBackend(""))

    //知识点2：容错机制相关配置(主要是设置checkpoint)
    env.enableCheckpointing(1000L) //此处的1000L指的时jobManager两次插入分界线的时间
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000L)
    // 下行设置的是前一次checkpoint距下一次分界线开始的最小时间(资源不能都用来设置检查点还要保证正常处理task)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)

    // 知识点3：重启策略(主要由以下两种)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 10000L))
    //    env.setRestartStrategy(RestartStrategies.noRestart())

    val inputStream: DataStream[String] = env.socketTextStream("warehousehadoop102", 7777)

    val mapTranDS: DataStream[SensorReading] = inputStream.map(
      line => {
        val strings: Array[String] = line.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
      })

    //知识点4：状态编程
    mapTranDS
      .keyBy(0)
      .flatMap(new TempChangeWaring(10.0)) //重点:对于DataStream和KeyedStream对象，均可以使用自定义状态函数实现状态定义(待商榷！！！)
      /*  .flatMapWithState[(String, Double, Double), Double] { //对于keyedstream还可以直接定义带状态的算子
        case (inputData, None) => (List.empty, Some(inputData.temperature))
        case (inputData, lastTemp) =>
          val diff = (inputData.temperature - lastTemp.get).abs
          if (diff > 10.0) {
            (List((inputData.id, lastTemp.get, inputData.temperature)), Some(inputData.temperature))
          } else {
            (List.empty, Some(inputData.temperature))
          }
      }*/
      .print().setParallelism(2) //此处设置并行度为2可以覆盖整体设置的并行度1


    env.execute("state test job")
  }
}


//自定义RichFlatMapFunction，结合状态编程实现温度跳变检测报警功能
class TempChangeWaring(interval: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  //① 创建表示上一个温度值的状态
  private var lastTempState: ValueState[Double] = _

  //②创建表示是否出现过当前传感器数据的标识位状态
  private var isOccurState: ValueState[Boolean] = _
  //  val defaultTemp: Double = -273.15

  //③在open方法中对状态进行声明和赋值(open方法在整个执行过程中只执行一次)
  //  解析：new ValueStateDescriptor为创建名称为XX，存储类型为XX，默认值为XX的状态
  //       lastTempState本质上为定义的状态变量，用于实时盛放根据上下文获得最新状态值

  override def open(parameters: Configuration): Unit = {
    //lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double], defaultTemp))  //与62行配合使用也可以控制第一次不输出
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
    isOccurState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-occur", classOf[Boolean])) //也有默认值，为null
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    val lastTemp: Double = lastTempState.value()
    val diff: Double = value.temperature - lastTemp

    if (isOccurState.value() && diff > interval) {
      //重点：Collector类的collect方法可以很方便地将满足要求的数据输出
      out.collect((value.id, lastTemp, value.temperature))
    }

    //更新状态
    lastTempState.update(value.temperature)
    isOccurState.update(true) //除了第一个后续全部设置为true
  }
}


//自定义监控状态的两种方式(以下定义没有实际意义)
//必须继承富函数，因为富函数中有getRuntimeContext等方法可以获取上下文
class MyMapStateOp extends RichMapFunction[SensorReading, String] {

  //①方式1：定义lazy属性，在被调用时再执行(不加lazy直接报错)
  lazy val MyValue: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("demoState", classOf[Int]))

  //②方式二：在open方法中定义，整个过程只执行一次
  override def open(parameters: Configuration): Unit = {
    val MyValue: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("demoState", classOf[Int]))
  }

  override def map(value: SensorReading): String = {
    MyValue.value()
    MyValue.update(10)
    ""
  }
}
