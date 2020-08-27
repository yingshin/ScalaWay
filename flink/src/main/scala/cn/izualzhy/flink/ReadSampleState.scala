package cn.izualzhy.flink

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.state.api.Savepoint
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.flink.util.Collector

/**
 * Description: 读取 StreamWithStateSample 写入的 State 数据.
 *
 */
object ReadSampleState extends App {
  val bEnv = ExecutionEnvironment.getExecutionEnvironment
  val params = ParameterTool.fromArgs(args)

  val restoredPath = params.get("ckdir")
  val existingSavepoint = Savepoint.load(
    bEnv,
    restoredPath,
    new MemoryStateBackend())

  val sampleCountStates = existingSavepoint.readKeyedState("count_uid", new SampleStateReaderFunction)
  sampleCountStates.printOnTaskManager("count_uid")
  val kafkaOffsetsState = existingSavepoint.readUnionState(
    "source_uid",
    "topic-partition-offset-states",
    TypeInformation.of(new TypeHint[Tuple2[KafkaTopicPartition, java.lang.Long]]() {}))
  kafkaOffsetsState.printOnTaskManager("source_uid")

  bEnv.execute()

  class SampleStateReaderFunction extends KeyedStateReaderFunction[java.lang.Integer, (Int, Long)] {
    var countState: ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
      val countStateDesc = new ValueStateDescriptor[Long]("count", createTypeInformation[Long])
      countState = getRuntimeContext.getState(countStateDesc)
    }

    override def readKey(key: java.lang.Integer, ctx: KeyedStateReaderFunction.Context, out: Collector[(Int, Long)]): Unit = {
      out.collect((key, countState.value()))
    }
  }
}
