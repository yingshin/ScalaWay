package cn.izualzhy.akka

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigSyntax}

import scala.collection.mutable
import scala.concurrent.duration._
import SampleConfig._

/**
 * Description:
 */

object SampleConfig {
  val WORKER_SYSTEM = "SampleWorker"
  val WORKER_ACTOR = "worker"
  val MASTER_SYSTEM = "SampleMaster"
  val MASTER_ACTOR = "master"
  val CHECK_INTERVAL = 10000
  val HEARTBEAT_INTERVAL = 10000
}

case class WorkerInfo(name: String)
case object IntervalCheck
case class Heartbeat(workerInfo: WorkerInfo)
case object SendHeartbeat

class Master(host: String, port: Int) extends Actor {
  val heartbeatsOfWorkers = new mutable.HashMap[WorkerInfo, Long]()

  override def preStart(): Unit = {
    import context.dispatcher
    context.system.scheduler.schedule(0 millis, CHECK_INTERVAL millis, self, IntervalCheck)
    println(s"master preStart path:${self.path} toString:${self.toString()}")
  }

  override def receive: Receive = {
    case Heartbeat(workerInfo) => {
      heartbeatsOfWorkers.update(workerInfo, System.currentTimeMillis())
      println(s"heartbeats from:${workerInfo} workers status:${heartbeatsOfWorkers.mapValues(ts2date)}")
    }
    case IntervalCheck => {
      val currentTime = System.currentTimeMillis()
      val inactiveWorkers = heartbeatsOfWorkers
        .filter{case (_, lastHeartbeat) => (currentTime - lastHeartbeat)> CHECK_INTERVAL}
        .mapValues(ts2date)
      println(s"inactiveWorkers: ${inactiveWorkers}")
    }
  }

  def ts2date(ts: Long) = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(ts))
}

object Master extends App {
  val host = args(0)
  val port = args(1).toInt

  val confStr =
    s"""
      |akka.actor.provider = akka.remote.RemoteActorRefProvider
      |akka.remote.netty.tcp.hostname = ${host}
      |akka.remote.netty.tcp.port = ${port}
      |""".stripMargin
  val config = ConfigFactory.parseString(confStr, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.PROPERTIES))
  val actorSystem = ActorSystem.create(MASTER_SYSTEM, config)
  val master = actorSystem.actorOf(Props(new Master(host, port)), MASTER_ACTOR)
  println(s"master.path:${master.path}")
  actorSystem.awaitTermination()
}

class Worker(workerInfo: WorkerInfo, masterHost: String, masterPort: Int) extends Actor {
  var master: ActorSelection = _

  override def preStart(): Unit = {
    master = context.actorSelection(s"akka.tcp://${MASTER_SYSTEM}@${masterHost}:${masterPort}/user/${MASTER_ACTOR}")
    import context.dispatcher
    context.system.scheduler.schedule(0 millis, HEARTBEAT_INTERVAL millis, self, SendHeartbeat)

    println(s"worker preStart path:${self.path} toString:${self.toString()} master:${master}")
  }

  override def receive: Receive = {
    case SendHeartbeat => {
      master ! Heartbeat(workerInfo)
      println(s"worker:${workerInfo} send heartbeat")
    }
  }
}

object Worker extends App {
  val masterHost = args(0)
  val masterPort = args(1).toInt
  val workerName = args(2)
  val workerPort = args(3)
  val workerTTL = args(4).toInt

  val confStr =
    s"""
      |akka.actor.provider = akka.remote.RemoteActorRefProvider
      |akka.remote.netty.tcp.port = ${workerPort}
      |""".stripMargin
  val config = ConfigFactory.parseString(confStr, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.PROPERTIES))
  val actorSystem = ActorSystem.apply(WORKER_SYSTEM, config)
  val worker = actorSystem.actorOf(Props(new Worker(WorkerInfo(workerName), masterHost, masterPort)), WORKER_ACTOR)
  println(s"worker.path:${worker.path}")

  Thread.sleep(workerTTL * 1000)

  actorSystem.shutdown()
  actorSystem.awaitTermination()
}
