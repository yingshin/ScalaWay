package cn.izualzhy.flink

import org.apache.flink.formats.csv.{CsvRowDeserializationSchema, CsvRowSerializationSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * Author: zhangying14
 * Date: 2020/8/25 16:56
 * Package: cn.izualzhy.flink
 * Description:
 *
 */
object RowSerialize extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val settings = EnvironmentSettings.newInstance()
    .useBlinkPlanner()
    .inStreamingMode()
    .build()
  val tableEnv = StreamTableEnvironment.create(env, settings)
  val source = env.fromCollection(List(
    (1, 1.23, null, true, 1L),
    (2, 1.1, "\"sf\"", false, 10L),
    (3, 1.02, "player", true, 100L),
    (4, 1.03, null, false, 1000L),
    (5, 1.23, "world", true, 10000L)))

  tableEnv.registerDataStream("source_table", source, 'a, 'b, 'c, 'd, 'e)
  val t2 = tableEnv.sqlQuery(
    """
      |SELECT * from source_table
      |""".stripMargin)

  val t2Stream = t2.toAppendStream[Row]
  val types = t2Stream.getType()
  println(s"types:${types}")
  t2Stream.addSink(r => {
    println("--------------------------")
    val seSchema = new CsvRowSerializationSchema.Builder(types)
      .setFieldDelimiter('|')
        .setNullLiteral("null")
        .build()
    val data = new String(seSchema.serialize(r))
    println(s"data:${data}")

    val deSchema = new CsvRowDeserializationSchema.Builder(types)
      .setFieldDelimiter('|')
        .setNullLiteral("null")
      .build()
    val deRow = deSchema.deserialize(data.getBytes)
    println(s"deRow:${deRow}")
    (0 until deRow.getArity).foreach{i => println(s"i:${i} v:|${deRow.getField(i) == null}|")}
  })

  env.execute()
}
