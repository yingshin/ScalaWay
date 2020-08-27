package cn.izualzhy.flink

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class StatefulFunctionWithTime extends KeyedProcessFunction[Int, Int, Void] {
  var state: ValueState[Int] = _
  var updateTimes: ListState[Long] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val stateDescriptor = new ValueStateDescriptor("state", createTypeInformation[Int])
//    val stateDescriptor = new ValueStateDescriptor("state", Types.INT)
    state = getRuntimeContext().getState(stateDescriptor)

    val updateDescriptor = new ListStateDescriptor("times", createTypeInformation[Long])
//    val updateDescriptor = new ListStateDescriptor("times", Types.LONG)
    updateTimes = getRuntimeContext().getListState(updateDescriptor)
  }

  @throws[Exception]
  override def processElement(value: Int, ctx: KeyedProcessFunction[ Int, Int, Void ]#Context, out: Collector[Void]): Unit = {
    state.update(value + 1)
    updateTimes.add(System.currentTimeMillis)
  }
}

object KeyedStateSample extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val fsStateBackend = new FsStateBackend("file:///tmp/chk_dir")
  env.setStateBackend(fsStateBackend)
  env.enableCheckpointing(60000)
  env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

  env.socketTextStream("127.0.0.1", 8010)
    .map(_.toInt)
    .keyBy(i => i)
    .process(new StatefulFunctionWithTime)
    .uid("my-uid")

  env.execute()
}

