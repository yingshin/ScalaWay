package cn.izualzhy.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.formats.csv.{CsvRowDataDeserializationSchema, CsvRowDataSerializationSchema}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.data.GenericRowData
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row

/**
 * Author: zhangying14
 * Date: 2020/9/14 16:48
 * Package: cn.izualzhy.flink
 * Description:
 *
 */
object Json2CsvFileSample extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
  val tEnv = StreamTableEnvironment.create(env, bsSettings)

  val csvDDL =
    """
      |CREATE TABLE csv_table (
      |  c1 ROW<word VARCHAR, upper_word VARCHAR>,
      |  c2 Row<word_len INT, upper_word_len INT>,
      |  c3 VARCHAR
      |) WITH (
      |  'connector' = 'filesystem',
      |  'path' = 'file:///tmp/csv.sink',
      |  'format' = 'csv',
      |  'sink.rolling-policy.rollover-interval' = '30s',
      |  'sink.rolling-policy.check-interval' = '2s'
      |)
      |""".stripMargin
  val ts = tEnv.executeSql(csvDDL)

  val row = GenericRowData.of(
    null,
    GenericRowData.of(Integer.valueOf(123), Integer.valueOf(123)),
    "c")
  val rowType = ts.getTableSchema.toRowDataType.getLogicalType.asInstanceOf[RowType]
  val serializationSchema = new CsvRowDataSerializationSchema.Builder(
    rowType).build()
  val serializedData = new String(serializationSchema.serialize(row))
  println(s"row:${row} serializedData:${serializedData}")

  val deserializationSchema = new CsvRowDataDeserializationSchema.Builder(
    rowType, new RowDataTypeInfo(rowType)).build()
  val deserializedRow = deserializationSchema.deserialize(serializedData.getBytes())
  println(s"data:${serializedData} deserialized:${deserializedRow}")

//  env.execute()
}
