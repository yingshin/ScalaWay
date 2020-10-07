package cn.izualzhy.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

/**
 * Author: zhangying14
 * Date: 2020/9/13 18:22
 * Package: cn.izualzhy.flink
 * Description:
 *
 */
object IdoitFlink extends App {
  val planner = "blink"

  // set up execution environment
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val tEnv = {  // use blink planner in streaming mode
    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    StreamTableEnvironment.create(env, settings)
  }

  val orderA: DataStream[Order] = env.fromCollection(Seq(
    Order(1L, "beer", 3),
    Order(1L, "diaper", 4),
    Order(3L, "rubber", 2)))

  val orderB: DataStream[Order] = env.fromCollection(Seq(
    Order(2L, "pen", 3),
    Order(2L, "rubber", 3),
    Order(4L, "beer", 1)))

  // convert DataStream to Table
  val tableA = tEnv.fromDataStream(orderA, 'user, 'product, 'amount)
  // register DataStream as Table
  tEnv.createTemporaryView("OrderB", orderB, 'user, 'product, 'amount)

  // union the two tables
  val result = tEnv.sqlQuery(
    s"""
       |SELECT * FROM $tableA WHERE amount > 2
       |UNION ALL
       |SELECT * FROM OrderB WHERE amount < 2
        """.stripMargin)

  result.toAppendStream[Order].print()

  env.execute()
  case class Order(user: Long, product: String, amount: Int)
}

