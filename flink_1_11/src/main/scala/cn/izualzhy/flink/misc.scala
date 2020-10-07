package cn.izualzhy.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Author: zhangying14
 * Date: 2020/9/23 19:40
 * Package: cn.izualzhy.flink
 * Description:
 *
 */
object misc {

  def createJsonFileTable(tEnv: StreamTableEnvironment, tableName: String, path: String) = {
    val jsonDDL =
      s"""
        |CREATE TABLE ${tableName} (
        |  c1 ROW<word VARCHAR, upper_word VARCHAR>,
        |  word_len INT
        |) WITH (
        |  'connector' = 'filesystem',
        |  'path' = 'file://${path}',
        |  'format' = 'json'
        |)
        |""".stripMargin
    tEnv.executeSql(jsonDDL)
  }

  def createCsvFileTable(tEnv: StreamTableEnvironment, tableName: String, path: String) = {
    val csvDDL =
      s"""
        |CREATE TABLE ${tableName} (
        | id INT,
        | name VARCHAR
        |) WITH (
        |  'connector' = 'filesystem',
        |  'path' = 'file://${path}',
        |  'format' = 'csv',
        |  'sink.rolling-policy.rollover-interval' = '30s',
        |  'sink.rolling-policy.check-interval' = '2s'
        |)
        |""".stripMargin
    tEnv.executeSql(csvDDL)
  }

}
