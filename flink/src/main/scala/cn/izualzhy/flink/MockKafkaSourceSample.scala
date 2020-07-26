package cn.izualzhy.flink

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.flink.api.scala._

/**
 * Author: zhangying14
 * Date: 2020/7/26 00:25
 * Package: cn.izualzhy.flink
 * Description:
 *
 */
object MockKafkaSource extends App {
  class MockKafkaStateSource extends SourceFunction[Int] with CheckpointedFunction {
    var unionOffsetStates: ListState[Tuple2[KafkaTopicPartition, Long]] = _
    override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
      Thread.sleep(300 * 1000)
    }

    override def cancel(): Unit = {}

    override def initializeState(functionInitializationContext: FunctionInitializationContext): Unit = {
      val stateStore = functionInitializationContext.getOperatorStateStore
      unionOffsetStates = stateStore.getUnionListState(new ListStateDescriptor(
        "topic-partition-offset-states",
        createTypeInformation[Tuple2[KafkaTopicPartition, Long]]
      ))

      for (unionOffsetState <- unionOffsetStates.get()) {
        println(s"f0:${unionOffsetState.f0} f1:${unionOffsetState.f1}")
        println(s"stackTrace:\n${Thread.currentThread.getStackTrace.mkString("\n")}")
      }
    }

    override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {
    }
  }

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val params = ParameterTool.fromArgs(args)
  val fsStateBackend = new FsStateBackend(params.get("ckdir"))
  env.setStateBackend(fsStateBackend)

  env.addSource(new MockKafkaStateSource).print()
  env.execute("ShowKafkaState")

}
