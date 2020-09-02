package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * @author xjp
  */

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val paraTool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = paraTool.get("host")
    val port: Int = paraTool.getInt("port")

    val inputDataStream: DataStream[String] = env.socketTextStream(host, port)

    val dataStream: DataStream[(String, Int)] = inputDataStream.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)

    dataStream.print().setParallelism(1)

    // 启动executor执行任务
    env.execute("socket stream wordcount")
  }
}
