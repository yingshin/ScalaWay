package cn.izualzhy.flink

import java.util.Collections

import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.operations.OutputConversionModifyOperation
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.datastream.{DataStream => JDataStream}
import org.apache.flink.streaming.api.transformations.OneInputTransformation

import collection.JavaConverters._

/**
 * Author: zhangying14
 * Date: 2020/8/4 10:24
 * Package: cn.izualzhy.flink
 * Description:
 *
 */
object SQLSample extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val settings = EnvironmentSettings.newInstance()
    .useBlinkPlanner()
    .inStreamingMode()
    .build()
  val tableEnv = StreamTableEnvironment.create(env, settings)
  val planner = tableEnv.asInstanceOf[StreamTableEnvironmentImpl].getPlanner

  val sellStream = env.socketTextStream("127.0.0.1", 8010)
    .map(line => {
      val tokens = line.split(" ")
      (tokens(0).toInt, tokens(1))
    })

  tableEnv.registerDataStream("sell_events_original", sellStream, 'product_id, 'product_name)

  val sourceStream = tableEnv.sqlQuery(
    """
      |SELECT UNIX_TIMESTAMP(), UNIX_TIMESTAMP(`product_name`, 'yyyyMMddHHmmss') FROM sell_events_original
      |""".stripMargin
  ).toAppendStream[Row].print("$")

  env.execute()
}
