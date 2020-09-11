package cn.izualzhy.flink

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.{CheckpointListener, FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.{CheckpointedFunction, ListCheckpointed}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

/**
 * Description:
 *
 */
object SourceSinkStateSample extends App {

  def logPrint(className: String)(msg: String) = {
    println(s"${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())} L${Thread.currentThread().getId} ${className} ${msg}")
  }

  class StateSource extends SourceFunction[String] with CheckpointedFunction with CheckpointListener {
    private val log = logPrint(getClass.getSimpleName)_
    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      log(s"${Thread.currentThread().getStackTrace}")
      (1 to 10000).foreach{i =>
        log(s"send:${i}")
        ctx.collect(i.toString)
        Thread.sleep(1000)
      }
    }

    override def cancel(): Unit = {}

    override def initializeState(context: FunctionInitializationContext): Unit = {
      log(s"initializeState")
    }
    override def snapshotState(context: FunctionSnapshotContext): Unit = {
//      log(s"snapshotState begin")
      log(s"snapshotState begin \n${Thread.currentThread().getStackTrace().mkString("\n")}")
      Thread.sleep(10000)
      log("snapshotState end")
    }

    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
      log(s"notifyCheckpointComplete ${checkpointId}")
    }

  }

  class StateSink extends SinkFunction[String] with CheckpointedFunction with CheckpointListener {
    private val log = logPrint(getClass.getSimpleName)_
    override def invoke(value: String): Unit = {
      log(s"recv:${value}\n${Thread.currentThread().getStackTrace().mkString("\n")}")
    }
    override def initializeState(context: FunctionInitializationContext): Unit = {
      log(s"initializeState")
    }
    override def snapshotState(context: FunctionSnapshotContext): Unit = {
//      log(s"snapshotState begin")
      log(s"snapshotState begin \n${Thread.currentThread().getStackTrace().mkString("\n")}")
      Thread.sleep(10000)
      log("snapshotState end")
    }
    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
      log(s"notifyCheckpointComplete ${checkpointId}")
    }
  }

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val fsStateBackend = new FsStateBackend("file:///tmp")
  env.setStateBackend(fsStateBackend)
  env.enableCheckpointing(60000)

  /*
  env.addSource(new StateSource)
    .addSink(new StateSink)
    .setParallelism(1)
   */

  env.addSource(new StateSource)
//    .keyBy(i => i)
    .addSink(new StateSink)
    .setParallelism(1)
  /*
   */

  env.execute(s"${getClass}")
}
