package cn.izualzhy.flink

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object KeyedStateSample extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
  val tEnv = StreamTableEnvironment.create(env, fsSettings)

  val sourceStream = env.socketTextStream("127.0.0.1", 8010)
      .map{v =>
        val columns = v.split(" ")
        (columns(0).toInt, columns(1), columns(2))
      }

//  val a = tEnv.fromDataStream(sourceStream, $"id", $"optype", $"tid")
  tEnv.registerDataStream("a", sourceStream, $"id", $"optype", $"tid")
  tEnv.sqlQuery(
    """
      |SELECT *, COLLECT(id) FROM a
      |""".stripMargin
  ).toAppendStream[Row].print()

  env.execute()
}

