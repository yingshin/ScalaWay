package cn.izualzhy.akka

import java.time.LocalDate
import java.util.Date

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension
import com.typesafe.config.ConfigFactory

/**
 * Description:
 *
 */
object QuartzSample extends App {
  val system = ActorSystem("SchedulerSystem")

  val scheduler = QuartzSchedulerExtension(system)
  case object Tick {
    var times = 0
    def next() = {
      times += 1
      Tick
    }
  }

  class WorkActor extends Actor {
    override def receive: Receive = {
      case Tick => {println(s"receive tick:${Tick.times} ${new Date}")}
      case _ => {println("receive _")}
    }
  }

  val worker = system.actorOf(Props[WorkActor])
  scheduler.createSchedule("Every30Seconds", None, "0/30 * * ? * *")
  val d = scheduler.schedule("Every30Seconds", worker, Tick.next())
  worker ! Tick
  println(s"d:${d}")
}
