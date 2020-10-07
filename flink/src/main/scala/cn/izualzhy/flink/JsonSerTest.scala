package cn.izualzhy.flink

import java.util
import java.util.{ArrayList, List}

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.formats.json.JsonRowSerializationSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row

/**
 * Author: zhangying14
 * Date: 2020/9/21 16:06
 * Package: cn.izualzhy.flink
 * Description:
 *
 */
object JsonSerTest extends App {
  val nestedFieldList = DataTypes.ROW(
    DataTypes.FIELD(
      "f0",
      DataTypes.ROW(
        DataTypes.FIELD("f0c0", DataTypes.INT()),
        DataTypes.FIELD("f0c1", DataTypes.STRING()))
    ),
    DataTypes.FIELD(
      "f1",
      DataTypes.ROW(
        DataTypes.FIELD("f1c0", DataTypes.INT()),
        DataTypes.FIELD("f1c1", DataTypes.STRING()))
    ),
    DataTypes.FIELD(
      "f2",
      DataTypes.BIGINT()
    )
  )
  val typeInfo = Types.ROW_NAMED(
    Array("f0", "f1", "f2"),
    Types.ROW_NAMED(
      Array("f0c0", "f0c1"),
      Types.INT, Types.STRING
    ),
    Types.ROW_NAMED(
      Array("f1c0", "f1c1"),
      Types.INT, Types.STRING
    ),
    Types.BIG_INT
  ).asInstanceOf[RowTypeInfo]

//  val typeInfo = TypeConversions.fromDataTypeToLegacyInfo(nestedFieldList).asInstanceOf[TypeInformation[Row]]
//  val typeInfo = TypeConversions.fromDataTypeToLegacyInfo(nestedFieldList).asInstanceOf[RowTypeInfo]
  val jsonRowSerializationSchema = new JsonRowSerializationSchema.Builder(typeInfo).build()
  val projectFields = Array(0, 2)
  val projectTypeInfo = RowTypeInfo.projectFields(typeInfo, projectFields)
  println(projectTypeInfo)
  val projectJsonRowSerializationSchema = new JsonRowSerializationSchema.Builder(projectTypeInfo).build()

  val r1 = Row.of(
    Row.of(Integer.valueOf(1), "hello"),
    Row.of(Integer.valueOf(2), "world"),
    java.lang.Long.valueOf(3)
  )
  ser(r1)

  val r2 = Row.of(
    Row.of(Integer.valueOf(1), "hello"),
    null,
    null
  )
  ser(r2)

  val arity = typeInfo.getArity
//  util.List[CompositeType.FlatFieldDescriptor] result = new util.ArrayList[CompositeType.FlatFieldDescriptor]
  println(typeInfo.getFlatFields("f1.*"))
  println(typeInfo.getFlatFields("f0.*"))
  println(typeInfo.getFlatFields("f0"))

  def ser(row: Row): Unit = {
    println(new String(jsonRowSerializationSchema.serialize(row)))
    println(new String(projectJsonRowSerializationSchema.serialize(Row.project(row, projectFields))))
  }

  val fieldsList = "*".split(",")
//  val fieldsList = "f\\d+,hello,scala".split(",")
  println(fieldsList.mkString(","))
  val names = Array("f0", "hello", "f1", "f2", "f3", "world", "fuck", "scala", "c++", "f100")
  val res = names.zipWithIndex.filter{ case (name, index) =>
    fieldsList.exists(field => name.matches(field))
  }.map(_._2)
  println(res.mkString(","))
}
