package cn.izualzhy.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * Author: zhangying14
 * Date: 2020/9/23 19:39
 * Package: cn.izualzhy.flink
 * Description:
 *
 */
object TestSQLPlan extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
  val tEnv = StreamTableEnvironment.create(env, bsSettings)
  val configuration = tEnv.getConfig().getConfiguration()
  configuration.setString("", "")
  tEnv.registerFunction("foo", new Foo)

  misc.createJsonFileTable(tEnv, "json_table", "/tmp/json")
  misc.createCsvFileTable(tEnv, "csv_table", "/tmp/csv")

  /*
  tEnv.sqlQuery(
    """
      |SELECT Row(new_id, new_name) as header, Row(new_id, new_name) as body
      |FROM (
      |SELECT foo(id) as new_id, foo(name) as new_name FROM csv_table
      |)
      |""".stripMargin
  ).toAppendStream[Row].print("$")
   */
  val t0 = tEnv.sqlQuery("""SELECT foo(id) as new_id, foo(name) as new_name FROM csv_table""")
  tEnv.registerTable("t0", t0)
  t0.printSchema()

  val t1 = tEnv.sqlQuery("""SELECT Row(new_id, new_name) as header, Row(new_id, new_name) as body FROM t0""")
  tEnv.registerTable("t1", t1)
  t1.printSchema()
  val t2 = tEnv.sqlQuery(
    """
      |SELECT header, body FROM t1
      |""".stripMargin)
  t2.toAppendStream[Row].print()

  print(env.getExecutionPlan)

  env.execute("")

  class Foo() extends ScalarFunction {
    def eval(column: Any): String = {
      println(s"foo column:${column}")
      "|" + column.toString + "|"
    }
  }
}
