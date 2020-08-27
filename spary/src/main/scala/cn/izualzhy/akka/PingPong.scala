package cn.izualzhy.akka

import akka.actor._

/**
 * Description: http://alvinalexander.com/scala/scala-akka-actors-ping-pong-simple-example/
 */

case object PingMessage
case object PongMessage
case object StartMessage
case object StopMessage

class Ping(pong: ActorRef) extends Actor {
  var count = 0
  def incAndPrint() = count += 1;println(s"ping recv pong count:${count}")

  override def preStart(): Unit = {
    println(s"preStart ${getClass}")
    super.preStart()
  }

  override def postStop(): Unit = {
    println(s"postStop ${getClass}")
    super.postStop()
  }

  override def receive: Receive = {
    case StartMessage =>
      incAndPrint()
      pong ! PingMessage
    case PongMessage =>
      incAndPrint()
      if (count > 99) {
        println(s"ping stop count:${count}")
        sender ! StopMessage
        context.stop(self)
      } else {
        sender ! PingMessage
      }
  }
}

class Pong() extends Actor {
  override def preStart(): Unit = {
    println(s"preStart ${getClass}")
    super.preStart()
  }

  override def postStop(): Unit = {
    println(s"postStop ${getClass}")
    println(context.parent)
    println(context.children.toList)
    println(context.props)
    super.postStop()
  }

  override def receive: Receive = {
    case PingMessage =>
      println("pong recv ping")
      sender ! PongMessage
    case StopMessage =>
      println("pong stop")
      context.stop(self)
  }
}

object PingPong extends App {
  val system = ActorSystem("MyPingPong")

  val pong = system.actorOf(Props[Pong], "pong")
  val ping = system.actorOf(Props(new Ping(pong)), "ping")

  println("path", ping.path)
  println("path", pong.path)

  ping ! StartMessage
}
