package cn.izualzhy.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * Author: zhangying14
 * Date: 2020/9/11 20:30
 * Package: cn.izualzhy.flink
 * Description:
 *
 */
object CsvRowSerializeSample extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.enableCheckpointing(6000)
  env.setParallelism(1)
  val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
  val tEnv = StreamTableEnvironment.create(env, bsSettings)

  val sourceStream = env.socketTextStream("127.0.0.1", 8010)
    .flatMap{line => line.split(" ").map(word => (word, word.length)) }
  tEnv.createTemporaryView("source_table", sourceStream, 'word, 'len)

  val countTable = tEnv.sqlQuery(
    s"""
      |SELECT ROW(word, word), ROW(len) FROM source_table
      |""".stripMargin)
//  countTable.toAppendStream[Row].addSink(new SinkFunction[Row] {
//    val csvSerializer = new CsvRowSerializationSchema.Builder(countTable.getSchema.toRowType).build()
//    override def invoke(value: Row): Unit = {
//      print(s"row:${value} csv:${new String(csvSerializer.serialize(value))}")
//    }
//  })

  val csvSinkTableDDL ="""
      |CREATE TABLE csv_sink (
      | c1 ROW<word1 VARCHAR, word2 VARCHAR>,
      | c2 ROW<`len` INT>
      |) WITH (
      | 'connector' = 'filesystem',
      | 'path' = 'file:///tmp/csv.sink.table',
      | 'format' = 'csv',
      | 'sink.rolling-policy.rollover-interval' = '30s',
      | 'sink.rolling-policy.check-interval' = '2s'
      |)
      |""".stripMargin
  tEnv.executeSql(csvSinkTableDDL)
  tEnv.sqlQuery(
    """
      |SELECT * FROM csv_sink
      |""".stripMargin).toAppendStream[Row].print("zyb: ")
//  countTable.executeInsert("csv_sink")

  env.execute()
}
