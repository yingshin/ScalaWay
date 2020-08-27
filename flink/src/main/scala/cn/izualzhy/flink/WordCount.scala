package cn.izualzhy.flink

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * Date: 2020/8/1 01:13
 * Description:
 *
 */
object WordCount extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val text = env.socketTextStream("127.0.0.1", 8011)

  text.flatMap(new FlatMapFunction[String, (String, Int)] {
    override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
      val tokens = value.toLowerCase().split("\\W+")
      tokens.foreach(token => out.collect((token, 1)))
    }
  }).setParallelism(1)
    .keyBy(0)
    .sum(1).setParallelism(1)
    .print()
  println(env.getStreamGraph)
  println(env.getExecutionPlan)

  env.execute("word_count")
}
