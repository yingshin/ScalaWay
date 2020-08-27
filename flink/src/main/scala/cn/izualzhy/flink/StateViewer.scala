package cn.izualzhy.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.memory.DataInputViewStreamWrapper
import org.apache.flink.runtime.checkpoint.StateObjectCollection
import org.apache.flink.runtime.state._
import org.apache.flink.state.api.runtime.SavepointLoader

import scala.collection.JavaConverters._

/**
 * Description:
 *
 */
object StateViewer extends App {
  val params = ParameterTool.fromArgs(args)
  val env = ExecutionEnvironment.getExecutionEnvironment
  val state = SavepointLoader.loadSavepoint(params.get("ckdir"))

  println(s"version:${state.getVersion} checkpointId:${state.getCheckpointId}")
  state.getOperatorStates.asScala.foreach{operatorState =>
    println(s"\toperatorState:${operatorState}")
    operatorState.getSubtaskStates.asScala.foreach{ case(subTaskIndex, subTaskState) =>
      println(s"\t\tsubTaskIndex:${subTaskIndex}")

      parseManagedOpState(subTaskState.getManagedOperatorState)
      parseManagedKeyedState(subTaskState.getManagedKeyedState)
    }
  }

  // unimplemented
  def parseManagedKeyedState(managedKeyedState: StateObjectCollection[KeyedStateHandle]) = {
    /*
    managedKeyedState.asScala.foreach{keyedStateHandle =>
      val keyGroupsStateHandle = keyedStateHandle.asInstanceOf[KeyGroupsStateHandle]
      val in = keyGroupsStateHandle.openInputStream()

      val inView = new DataInputViewStreamWrapper(in)
      val serializationProxy = new KeyedBackendSerializationProxy[Any](Thread.currentThread().getContextClassLoader)
      serializationProxy.read(inView)

      val restoredMetaInfos = serializationProxy.getStateMetaInfoSnapshots.asScala
      val kvStatesById = restoredMetaInfos.map{stateMetaInfoSnapshot =>
        println(s"\t\t\t\tname:${stateMetaInfoSnapshot.getName} type:${stateMetaInfoSnapshot.getBackendStateType}")
        stateMetaInfoSnapshot
      }.zipWithIndex

      val streamCompressionDecorator = if (serializationProxy.isUsingKeyGroupCompression) {
        SnappyStreamCompressionDecorator.INSTANCE
      } else {
        UncompressedStreamCompressionDecorator.INSTANCE
      }

      keyGroupsStateHandle.getGroupRangeOffsets.forEach{case (keyGroupIndex, offset) =>
        in.seek(offset)
        val writtenKeyGroupIndex = inView.readInt()
        val kgCompressionInStream = streamCompressionDecorator.decorateWithCompression(in)

        val kgCompressionInStreamView = new DataInputViewStreamWrapper(kgCompressionInStream)
        restoredMetaInfos.zipWithIndex.foreach{case(stateMetaInfoSnapshot, index) =>
          val kvStateId = kgCompressionInStreamView.readShort()
        }
      }
    }
     */
  }

  // copy from OperatorStateRestoreOperation.restore
  def parseManagedOpState(managedOperatorState: StateObjectCollection[OperatorStateHandle]) = {
    managedOperatorState.asScala.foreach{operatorStateHandle =>
      val serializationProxy = new OperatorBackendSerializationProxy(Thread.currentThread().getContextClassLoader)
      serializationProxy.read(
        new DataInputViewStreamWrapper(operatorStateHandle.openInputStream())
      )

      val nameToSerializer = serializationProxy.getOperatorStateMetaInfoSnapshots().asScala.map{stateMetaInfoSnapshot =>
        println(s"\t\t\t\tname:${stateMetaInfoSnapshot.getName} type:${stateMetaInfoSnapshot.getBackendStateType}")
        //          val restoredMetaInfo = new RegisteredOperatorStateBackendMetaInfo[_](stateMetaInfoSnapshot)
        val restoredMetaInfo = new RegisteredOperatorStateBackendMetaInfo[Any](stateMetaInfoSnapshot)
        restoredMetaInfo.getName -> restoredMetaInfo.getPartitionStateSerializer
      }.toMap

      val in = operatorStateHandle.openInputStream()
      operatorStateHandle.getStateNameToPartitionOffsets.asScala.foreach{case (name, metaInfo) =>
        val inView = new DataInputViewStreamWrapper(in)
        val serializer = nameToSerializer.get(name).get
        val offsets = metaInfo.getOffsets

        println(s"\t\t\t\tname:${name} serializer:${serializer} offsets:${offsets.mkString("[", ",", "]")}")
        offsets.foreach{offset =>
          in.seek(offset)
          println(s"\t\t\t\t\toffset:${offset} value:${serializer.deserialize(inView)}")
        }
      }
    }
  }
}
