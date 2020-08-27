package cn.izualzhy.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * Description: http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/How-to-retain-the-column-name-when-convert-a-Table-to-DataStream-tp37002.html
 *
 */
object StreamTableWrongColName extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val settings = EnvironmentSettings.newInstance()
    .useBlinkPlanner()
    .inStreamingMode()
    .build()
  val tableEnv = StreamTableEnvironment.create(env, settings)

  val sourceStream = env.socketTextStream("127.0.0.1", 8010)
    .map(line => line.toInt)
  tableEnv.registerDataStream("source_table", sourceStream, 'a)
//  tableEnv.registerDataStream("source_table", sourceStream, 'a, 'proctime.proctime)

  class Foo() extends TableFunction[(Int)] {
    def eval(col: Int): Unit = collect((col * 10))
  }
  tableEnv.registerFunction("foo", new Foo)

  val t1 = tableEnv.sqlQuery(
    """
      |SELECT source_table.*, b FROM source_table
      |, LATERAL TABLE(foo(a)) as T(b)
      |""".stripMargin
  )
  /*
   t1 table schema: root
   |-- a: INT
   |-- b: INT
   */
  println(s"t1 table schema: ${t1.getSchema}")
//  val t1Stream = t1.toAppendStream[Row](t1.getSchema.toRowType)
  val t1Stream = t1.toAppendStream[Row]
  // t1 stream schema: Row(a: Integer, f0: Integer)
  println(s"t1 stream schema: ${t1Stream.getType()}")
  tableEnv.registerDataStream("t1", t1Stream)
//  tableEnv.registerDataStream("t1", t1Stream, 'a, 'b)

//  println(tableEnv.listTables().mkString(","))
//  println(tableEnv.listUserDefinedFunctions().mkString(","))
  /*
  new t1 table schema: root
  |-- a: INT
  |-- f0: INT
   */
  println(s"new t1 table schema: ${tableEnv.scan("t1").getSchema}")

  // Error: ... At line 3, column 21: Column 'b' not found in any table
//  val t2 = tableEnv.sqlQuery(
//    """
//      |SELECT t1.*, c FROM t1
//      |, LATERAL TABLE(foo(b)) as T(c)
//      |""".stripMargin
//  )
//
//  println(s"t2 table schema: ${t2.getSchema}")
//  val t2Stream = t2.toAppendStream(t2.getSchema.toRowType)
//  val t2Stream = t2.toAppendStream[Row]
//  println(s"t2 stream schema: ${t2Stream.getType()}")

  env.execute()

}
