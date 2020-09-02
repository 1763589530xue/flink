package com.atguigu.api.sinktest

import java.sql.{Connection, Driver, DriverManager, PreparedStatement}

import com.atguigu.api.SensorReading
import org.apache.flink.api.common.functions.{IterationRuntimeContext, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
  * @author xjp
  */

// 将数据存储到Mysql中
// 现有包中没有jdbc sink相关的描述，因此需要完全自定义
object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(1)

    val inputStream: DataStream[String] = env.readTextFile("H:\\IDEA\\Flink\\flinkTutorial\\src\\source.txt")

    val mapTranDS: DataStream[SensorReading] = inputStream.map(
      line => {
        val strings: Array[String] = line.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
      })

    mapTranDS.addSink(new MyJdbcSink())

    env.execute("jdbc sink job")
  }
}

class MyJdbcSink() extends RichSinkFunction[SensorReading] {
  //定义sql连接、预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  //执行过程中只调用一次
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://warehousehadoop102:3306/test", "root", "123456")
    insertStmt = conn.prepareStatement("insert into sensor_temp (id, temperature) values(?,?)")
    updateStmt = conn.prepareStatement("update table sensor_temp set temperature = ? where id = ?")
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()

    // 业务需求：如果没有更新则将该数据插入
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    conn.close()
    insertStmt.close()
    updateStmt.close()
  }
}
