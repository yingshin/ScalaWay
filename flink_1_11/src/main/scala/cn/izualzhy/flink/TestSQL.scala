package cn.izualzhy.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 * Author: zhangying14
 * Date: 2020/9/14 16:48
 * Package: cn.izualzhy.flink
 * Description:
 *
 */
object TestSQL extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.enableCheckpointing(6000)
  env.setParallelism(1)
  val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
  val tEnv = StreamTableEnvironment.create(env, bsSettings)

  val jsonDDL =
    """
      |CREATE TABLE json_table (
      |  c1 ROW<word VARCHAR, upper_word VARCHAR>,
      |  word_len INT
      |) WITH (
      |  'connector' = 'filesystem',
      |  'path' = 'file:///tmp/json.source',
      |  'format' = 'json'
      |)
      |""".stripMargin
  tEnv.executeSql(jsonDDL)
  val csvDDL =
    """
      |CREATE TABLE csv_table (
      |  c1 ROW<word VARCHAR, upper_word VARCHAR>,
      |  word_len INT
      |) WITH (
      |  'connector' = 'filesystem',
      |  'path' = 'file:///tmp/csv.sink',
      |  'format' = 'csv',
      |  'sink.rolling-policy.rollover-interval' = '30s',
      |  'sink.rolling-policy.check-interval' = '2s'
      |)
      |""".stripMargin
  tEnv.executeSql(csvDDL)

  tEnv.sqlQuery(
    """
      |SELECT * FROM csv_table
      |""".stripMargin).toAppendStream[Row].addSink(new SinkFunction[Row] {
    override def invoke(value: Row): Unit = {
      println(s"stack:${Thread.currentThread().getStackTrace.mkString("\n")}")
      println(s"value:${value}")
    }
  })
//    .executeInsert("csv_table")

  env.execute()
}
