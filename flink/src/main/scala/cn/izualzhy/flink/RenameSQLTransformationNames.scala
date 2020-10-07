package cn.izualzhy.flink

import java.util.Collections

import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.operations.OutputConversionModifyOperation
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.datastream.{DataStream => JDataStream}
import org.apache.flink.streaming.api.transformations.OneInputTransformation

import collection.JavaConverters._

object RenameSQLTransformationNames extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val settings = EnvironmentSettings.newInstance()
    .useBlinkPlanner()
    .inStreamingMode()
    .build()
  val tableEnv = StreamTableEnvironment.create(env, settings)
  val planner = tableEnv.asInstanceOf[StreamTableEnvironmentImpl].getPlanner

  def hackToDataStream[T](table: Table, opName: String) = {
    val op = new OutputConversionModifyOperation(
      table.getQueryOperation,
      table.getSchema.toRowDataType,
      OutputConversionModifyOperation.UpdateMode.APPEND)
    val transform: Transformation[T] = planner.translate(Collections.singletonList(op)).asScala.head.asInstanceOf[Transformation[T]]
    println(s"original name:${transform.getName} ${transform}")
    transform.setName(opName + "_out_s")

    transform.asInstanceOf[OneInputTransformation[_, _]]
      .getInput
      .setName(opName)
    transform.asInstanceOf[OneInputTransformation[_, _]]
      .getInput
      .asInstanceOf[OneInputTransformation[_, _]]
      .getInput
      .setName(opName + "_in_s")

    env.getWrappedStreamExecutionEnvironment.addOperator(transform)

    new DataStream[T](new JDataStream[T](
      env.getWrappedStreamExecutionEnvironment, transform))
  }

  case class SellEvent(product_id: Int, product_name: String)
  val sellStream = env.socketTextStream("127.0.0.1", 8010)
    .map(line => {
      val tokens = line.split(" ")
      SellEvent(tokens(0).toInt, tokens(1))
    })

  tableEnv.registerDataStream("sell_events_original", sellStream, 'product_id, 'product_name)

  val t = tableEnv.scan("sell_events_original")
  val tStream = hackToDataStream(t, "source_cached")
//  val tStream = t.toAppendStream[Row]
//  val sourceStream = tableEnv.sqlQuery(
//    """
//      |SELECT product_id + 1 as product_id, product_name FROM sell_events_original
//      |""".stripMargin
//  )
  //  tableEnv.registerTable("source_table", sourceStream)
  tableEnv.registerDataStream("source_table", tStream)
//  tableEnv.registerDataStream("source_table", hackToDataStream(sourceStream, "source_table"))
  val sinkTableDDL =
    """
      |CREATE TABLE csv_sink_table (
      | c1 INT,
      | c2 VARCHAR
      |) WITH (
      | 'format.type' = 'csv',
      | 'connector.type' = 'filesystem',
      | 'connector.path' = 'file:///tmp/test',
      | 'update-mode' = 'append',
      | 'format.fields.0.name' = 'c1',
      | 'format.fields.0.type' = 'INT',
      | 'format.fields.1.name' = 'c2',
      | 'format.fields.1.type' = 'VARCHAR'
      |)
      |""".stripMargin
  tableEnv.sqlUpdate(sinkTableDDL)


  val t1 = tableEnv.sqlQuery(
    """
      |SELECT product_id * 2 as product_id, product_name FROM source_table
      |""".stripMargin
  )
  //  tableEnv.registerTable("t1", t1)
//  val t1Stream = t1.toAppendStream[Row]
  val t1Stream = hackToDataStream[Row](t1, "t1")
  tableEnv.registerDataStream("t1", t1Stream)

  tableEnv.sqlUpdate(
    """
      |INSERT INTO csv_sink_table SELECT product_id + 3 as product_id, product_name FROM t1
      |""".stripMargin)

  /*
  val resTable = tableEnv.sqlQuery(
    """
      |SELECT product_id + 3 as product_id, product_name FROM t1
      |""".stripMargin
  )
  hackToDataStream(resTable, "res").print()
   */

  println(env.getExecutionPlan)
  val sg = env.getStreamGraph
  sg.getStreamNodes.asScala.zipWithIndex.foreach{ case (node, i) =>
    println(i, node.getOperatorName)
  }
  sg.getStreamNode(3).getOperatorName

  env.execute()
}
