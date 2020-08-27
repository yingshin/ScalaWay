package cn.izualzhy.flink

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition

import scala.collection.JavaConversions._
/**
 * Author: zhangying14
 * Date: 2020/7/26 00:25
 * Package: cn.izualzhy.flink
 * Description:
 *
 */
object MockKafkaSourceSample extends App {
  class MockKafkaSource extends RichParallelSourceFunction[String] with CheckpointedFunction {
    var unionOffsetStates: ListState[Tuple2[KafkaTopicPartition, Long]] = _
    override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
      (1 to 100).foreach{_ =>
        (1 to 6).foreach{i =>
          sourceContext.collect(i.toString)
          Thread.sleep(5000)
        }
      }
    }

    override def cancel(): Unit = {}

    override def initializeState(functionInitializationContext: FunctionInitializationContext): Unit = {
      println("initializeState")
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
  val fsStateBackend = new FsStateBackend(params.get("ckdir", "file:///tmp"))
  env.setStateBackend(fsStateBackend)
  env.enableCheckpointing(60000)
  env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

  env.addSource(new MockKafkaSource).uid("source_uid")
    .map(_.toInt)
    .keyBy(i => i)
    .map(new CountFunction)
    .uid("count_uid")
    .print()

  env.execute("MockKafkaState")

  class CountFunction extends RichMapFunction[Int, (Int, Long)] {
    var countState: ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
      val countStateDesc = new ValueStateDescriptor[Long]("count", createTypeInformation[Long])
      countStateDesc.setQueryable("count_query_uid")
      countState = getRuntimeContext.getState(countStateDesc)
    }


    override def map(value: Int): (Int, Long) = {
      val currentCount = countState.value()
      val newCount = if (currentCount != null) currentCount + 1 else 1L

      countState.update(newCount)
      (value, newCount)
    }
  }
}
