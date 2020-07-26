package cn.izualzhy.flink

import java.util.Properties

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

/**
 * Description:
 *
 */
object StateSample extends App {
  val params = ParameterTool.fromArgs(args)
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val fsStateBackend = new FsStateBackend(params.get("ckdir"))
  env.setStateBackend(fsStateBackend)
  env.enableCheckpointing(60000)
  env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", s"${params.get("brokers")}")
  val consumer = new FlinkKafkaConsumer011(
    params.get("source_topic"),
    new JSONKeyValueDeserializationSchema(false),
    properties
  )

  env.addSource(consumer)
    .uid("source_uid")
    .map(_.get("value").get("weight").asInt())
    .keyBy(i => i)
    .map(new CountFunction)
    .uid("count_uid")
    .print()

  env.execute()

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
