package cn.izualzhy.flink

import cn.izualzhy.flink.CsvRowDataSerDeSchemaTest.normalRowData
import org.apache.flink.formats.csv.{CsvRowDataDeserializationSchema, CsvRowDataSerializationSchema}
import org.apache.flink.table.api.DataTypes._
import org.apache.flink.table.data.binary.BinaryStringData
import org.apache.flink.table.data.{GenericRowData, RowData, StringData}
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo
import org.apache.flink.table.types.logical.RowType
/**
 * Author: zhangying14
 * Date: 2020/9/15 11:13
 * Package: cn.izualzhy.flink
 * Description:
 *
 */
object CsvRowDataSerDeSchemaTest extends App {
  val subDataType0 = ROW(
    FIELD("f0c0", STRING()),
    FIELD("f0c1", STRING())
  )
  val subDataType1 = ROW(
    FIELD("f1c0", INT()),
    FIELD("f1c1", INT())
  )
  val datatype = ROW(
    FIELD("f0", subDataType0),
    FIELD("f1", subDataType1))
  val rowType = datatype.getLogicalType.asInstanceOf[RowType]

  val serSchema = new CsvRowDataSerializationSchema.Builder(rowType).build()
  val deserSchema = new CsvRowDataDeserializationSchema.Builder(rowType, new RowDataTypeInfo(rowType)).build()
  def foo(r: RowData): Unit = {
    val serData = new String(serSchema.serialize(r))
    print(s"${serData}")
    val deserRow = deserSchema.deserialize(serData.getBytes)
    println(s"${deserRow}")
  }

  val normalRowData = GenericRowData.of(
    GenericRowData.of(BinaryStringData.fromString("hello"), BinaryStringData.fromString("world")),
    GenericRowData.of(Integer.valueOf(123), Integer.valueOf(456))
  )
  // correct.
  foo(normalRowData)

  val nullRowData = GenericRowData.of(
    null,
    GenericRowData.of(Integer.valueOf(123), Integer.valueOf(456))
  )
  /*
  Exception in thread "main" java.io.IOException: Failed to deserialize CSV row ',123;456
  ...
  Caused by: java.lang.RuntimeException: Row length mismatch. 2 fields expected but was 0.
   */
  foo(nullRowData)
}
