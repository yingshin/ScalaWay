import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.state.api.Savepoint
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
 * Description:
 */
object TestReadState extends App {
  val bEnv      = ExecutionEnvironment.getExecutionEnvironment
  val savepoint = Savepoint.load(bEnv, "file:///tmp/chk_dir/be3a0afedce186700f6fa10dc1b9a0bb/chk-1", new MemoryStateBackend)
  val keyedState = savepoint.readKeyedState("my-uid", new ReaderFunction)
  keyedState.print()

  case class KeyedState(key: Int, value: Int, times: List[Long])
  class ReaderFunction extends KeyedStateReaderFunction[java.lang.Integer, KeyedState] {
    var state: ValueState[Int] = _
    var updateTimes: ListState[Long] = _

    @throws[Exception]
    override def open(parameters: Configuration): Unit = {
      val stateDescriptor = new ValueStateDescriptor("state", createTypeInformation[Int])
      state = getRuntimeContext().getState(stateDescriptor)

      val updateDescriptor = new ListStateDescriptor("times", createTypeInformation[Long])
      updateTimes = getRuntimeContext().getListState(updateDescriptor)
    }

    override def readKey(key: java.lang.Integer,
                         ctx: KeyedStateReaderFunction.Context,
                         out: Collector[KeyedState]): Unit = {
      val data = KeyedState(
        key,
        state.value(),
        updateTimes.get().asScala.toList)
      out.collect(data)
    }
  }
}
