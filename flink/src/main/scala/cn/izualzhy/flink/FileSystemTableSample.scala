package cn.izualzhy.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._

/**
 * Author: zhangying14
 * Date: 2020/9/10 17:17
 * Package: cn.izualzhy.flink
 * Description:
 *
 */
object FileSystemTableSample extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
  val tEnv = StreamTableEnvironment.create(env, fsSettings)

  val sourceStream = env.socketTextStream("127.0.0.1", 8010)
    .map{v =>
      val columns = v.split(" ")
      (columns(0).toInt, columns(1), columns(2))
    }

  tEnv.registerDataStream("a", sourceStream, 'id, 'optype, 'tid)

  val fileSinkTable =
    """
      |CREATE TABLE dummy_file_table (
      | id INT,
      | op_type VARCHAR,
      | tid VARCHAR
      |) WITH (
      | 'connector.type' = 'filesystem',
      | 'connector.path' = 'file://tmp/dummy_file_table',
      | 'format.type' = 'json',
      | 'format.derive-schema' = 'true'
      |)
      |""".stripMargin
  tEnv.sqlUpdate(fileSinkTable)

  tEnv.sqlQuery("SELECT * FROM a")
      .addSink()

  env.execute("test")

}
