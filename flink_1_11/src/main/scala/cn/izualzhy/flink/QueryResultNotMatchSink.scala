package cn.izualzhy.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._

/**
 * Author: zhangying14
 * Date: 2020/9/21 20:49
 * Package: cn.izualzhy.flink
 * Description:
 *
 */
object QueryResultNotMatchSink extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
  val tEnv = StreamTableEnvironment.create(env, bsSettings)

  val sourceStream = env.socketTextStream("127.0.0.1", 8010)
    .map{line =>
      val tokens = line.split(" ")
      (tokens(0).toInt, tokens(1))
    }
  tEnv.createTemporaryView("source_table", sourceStream, 'id, 'name)

  val jsonTableDDL ="""
                         |CREATE TABLE tbl_json (
                         | c1 ROW<word1 VARCHAR, word2 VARCHAR>,
                         | c2 ROW<`len` INT>
                         |) WITH (
                         | 'connector' = 'filesystem',
                         | 'path' = 'file:///tmp/csv.sink.table',
                         | 'format' = 'json'
                         |)
                         |""".stripMargin
  tEnv.executeSql(jsonTableDDL)

  tEnv.executeSql(
    """
      |INSERT INTO tbl_json SELECT ROW(name, id), Row(id) FROM source_table
      |""".stripMargin)

}
