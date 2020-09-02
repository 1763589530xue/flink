package com.atguigu.api

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @author xjp
  */

//底层ProcessFunction函数的使用(process函数主要可以提取watermark和设置闹铃)
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("warehousehadoop102", 7777)
    val mapTranDS: DataStream[SensorReading] = inputStream.map(
      line => {
        val strings: Array[String] = line.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
      })

    //需求：检测10秒内温度是否连续上升，如果上升则报警；
    val resDS: DataStream[String] = mapTranDS
      .keyBy("id")
      .process(new TempIncreaseWarining(10000L))

    resDS.print()

    env.execute(" process function test")
  }
}


//自定义KeyedProcessFunction(该类继承于AbstractRichFunction，因此包含了富函数中的常用方法[主要是获取上下文])
class TempIncreaseWarining(interval: Long) extends KeyedProcessFunction[Tuple, SensorReading, String] {
  //1.定义上一个温度状态和当前定时器时间戳状态(方便取消定时器)
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  lazy val curTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("current-timer-ts", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
    //2.取出状态
    val lastTemp: Double = lastTempState.value()
    val curTimerTs: Long = curTimerTsState.value()
    //3.更新lastTempState的状态
    lastTempState.update(value.timestamp)

    //4.如果温度上升并且没有定时器，则注册一个10秒后的定时器
    val diff: Double = value.timestamp - lastTemp
    if (diff > 0 && curTimerTs == 0) {
      //此处直接使用process time
      val ts: Long = ctx.timerService().currentProcessingTime() + interval

      // 创建定时器的写法(需要使用到上下文对象)
      ctx.timerService().registerProcessingTimeTimer(ts)

      // 更新curTimerTs的状态
      curTimerTsState.update(ts)
    } else if (diff < 0) {
      // 如果温度下降则删除之前定义的定时器，并清空状态(否则无法创建新的定时器)
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)

      curTimerTsState.clear()


      // 扩展：获取水位线的方式(本质：根据上下文获取)
      //      ctx.timerService().currentWatermark()
      //      ctx.timerService().currentWatermark()
      //      ctx.timerService().currentWatermark()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    //执行到此说明定时器被触发
    out.collect(s"传感器 ${ctx.getCurrentKey} 的温度值已经连续 ${interval / 1000} 秒上升了")

    //需要清空定时器的时间戳状态
    curTimerTsState.clear()
  }
}
