package com.atguigu.api.sinktest

import java.util

import com.atguigu.api.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  * @author xjp
  */

// 将数据保存在ES上
object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(1)

    val inputStream: DataStream[String] = env.readTextFile("H:\\IDEA\\Flink\\flinkTutorial\\src\\source.txt")

    val mapTranDS: DataStream[SensorReading] = inputStream.map(
      line => {
        val strings: Array[String] = line.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
      })

    //  写入es
    //  定义HttpHost
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("warehousehadoop102", 9200))

    mapTranDS.addSink(new ElasticsearchSink.Builder[SensorReading](
      httpHosts, new MyEsSinkFunction)
      .build())

    env.execute("es sink job")
  }
}

// 定义一个EsSinkFunction
class MyEsSinkFunction extends ElasticsearchSinkFunction[SensorReading] {
  override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    //  提取数据包装source(类型为map类型，因为ES只能存储KV格式的数据)
    val dataSource = new util.HashMap[String, String]()
    dataSource.put("id", t.id)
    dataSource.put("temp", t.temperature.toString)
    dataSource.put("ts", t.timestamp.toString)

    //   创建index request
    val indexRequest: IndexRequest = Requests.indexRequest()
      .index("sensor")
      .`type`("temperature")
      .source(dataSource)

    // 利用RequestIndexer对象发送http请求
    requestIndexer.add(indexRequest)

    println(t + " is saved")
  }
}
