package cn.izualzhy.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableSchema}
import org.apache.flink.table.functions.{AsyncTableFunction, TableFunction}
import org.apache.flink.table.sources.LookupableTableSource
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row

/**
 * Description:
 */
object TemporalTablesSample extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val settings = EnvironmentSettings.newInstance()
    .useBlinkPlanner()
    .inStreamingMode()
    .build()
  val tableEnv = StreamTableEnvironment.create(env, settings)
  tableEnv.registerTableSource("tblDim", new MockDimTable)

  case class Item(id: Int)
  val sourceStream = env.socketTextStream("127.0.0.1", 8010)
    .map(line => Item(line.toInt))
  tableEnv.registerDataStream("sourceTable", sourceStream, 'item_id, 'proc_time.proctime)

  val sql =
    """
      |SELECT sourceTable.item_id, tblDim.item_id, tblDim.item_name, sourceTable.proc_time
      |FROM sourceTable JOIN tblDim FOR SYSTEM_TIME AS OF sourceTable.proc_time
      |ON sourceTable.item_id = tblDim.item_id
      |""".stripMargin
//  ORDER BY tblDim.item_name
  tableEnv.sqlQuery(sql).toAppendStream[Row].print()
  env.execute()

  class MockDimTable extends LookupableTableSource[Row] {
    val tableSchema = TableSchema.builder()
        .field("item_id", DataTypes.INT())
        .field("item_name", DataTypes.STRING())
        .build()

    override def getAsyncLookupFunction(lookupKeys: Array[String]): AsyncTableFunction[Row] = ???
    override def getLookupFunction(lookupKeys: Array[String]): TableFunction[Row] = new MockTableFunction

    override def isAsyncEnabled: Boolean = false
    override def getTableSchema: TableSchema = tableSchema

    override def getProducedDataType: DataType = tableSchema.toRowDataType
  }

  class MockTableFunction extends TableFunction[Row] {
//    def eval(id: java.lang.Integer) = {
//    def eval(id: AnyRef) = {
    def eval(id: Int) = {
      /*
      Error:(60, 22) type mismatch;
       found   : Int
       required: Object
      Note: an implicit exists from scala.Int => java.lang.Integer, but
      methods inherited from Object are rendered ambiguous.  This is to avoid
      a blanket implicit which would convert any scala.Int to any AnyRef.
      You may wish to use a type ascription: `x: java.lang.Integer`.
       */
//      collect(Row.of(id, "banana"))
//      collect(Row.of(id, "cat"))
//      collect(Row.of(id, "apple"))
//      collect(Row.of(id, "dog"))
    }
  }
}
