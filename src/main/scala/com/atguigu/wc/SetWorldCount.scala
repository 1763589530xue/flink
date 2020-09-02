package com.atguigu.wc


import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
  * @author xjp
  */

//Flink也可以进行批处理
object SetWorldCount {
  def main(args: Array[String]): Unit = {
    // flink处理的第一步是获取运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val set: DataSet[String] = env.readTextFile("H:\\IDEA\\Flink\\flinkTutorial\\src\\words.txt")

    val worldCountDS: AggregateDataSet[(String, Int)] = set.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    worldCountDS.print()

  }
}
