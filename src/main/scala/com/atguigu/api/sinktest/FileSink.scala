package com.atguigu.api.sinktest

import com.atguigu.api.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

/**
  * @author xjp
  */

//本地文件和HDFS路径上文件存储方式
object FileSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)

    val inputStream: DataStream[String] = env.readTextFile("H:\\IDEA\\Flink\\flinkTutorial\\src\\source.txt")

    val mapTranDS: DataStream[SensorReading] = inputStream.map(
      line => {
        val strings: Array[String] = line.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
      })

    mapTranDS.addSink(
      // 使用的是传入SinkFunction的方式
      StreamingFileSink.forRowFormat(
        new Path("H:\\IDEA\\Flink\\flinkTutorial\\src\\out.txt"),
        new SimpleStringEncoder[SensorReading]("UTF-8")
      ).build()
    )

    env.execute("file sink job")
  }
}
