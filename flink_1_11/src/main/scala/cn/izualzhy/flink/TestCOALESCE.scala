package cn.izualzhy.flink

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
import org.apache.flink.types.Row
import org.slf4j.{Logger, LoggerFactory}

/**
 * Author: zhangying14
 * Date: 2020/9/27 11:26
 * Package: cn.izualzhy.flink
 * Description:
 *
 */
object TestCOALESCE extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
  val tEnv = StreamTableEnvironment.create(env, bsSettings)

  val source_stream = env.socketTextStream("127.0.0.1", 8010)
  tEnv.createTemporaryView("source_table", source_stream, 'a)
  tEnv.registerFunction("foo", new Foo)

  tEnv.sqlQuery(
    """
      |SELECT COALESCE(foo(a), 'default') FROM source_table
      |""".stripMargin).toAppendStream[Row].print("$")

  class Foo extends ScalarFunction {
    var logger: Logger = _
    var count: Int = 0
    override def open(context: FunctionContext): Unit = {
      logger = LoggerFactory.getLogger(this.getClass.getCanonicalName)
      println(s"Foo.open logger:${logger}")
    }

    def eval(column: String): String = {
      count += 1
//      logger.info(s"Foo.eval ${column}")
      println(s"Foo.eval ${column}")
      if (column.toInt >= 0) s"column:${column} count:${count}"
      else null
    }
  }

//  tEnv.executeSql("SELECT * FROM source_table").print()

  env.execute(this.getClass.getCanonicalName)

}
