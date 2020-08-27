package cn.izualzhy.akka

import akka.actor.{Actor, ActorSystem, Props}

/**
 * Description:
 * http://alvinalexander.com/scala/simple-scala-akka-actor-examples-hello-world-actors/
 *
 */
class HelloActor extends Actor {
  override def receive: Receive = {
    case "hello" => println("hello back at you")
    case _ => println("huh?")
  }
}

object HelloWorld extends App {
  val system = ActorSystem("HelloSystem")
  // default Actor constructor
  val helloActor = system.actorOf(Props[HelloActor], "hello_actor")
  helloActor ! "hello"
  helloActor ! "buenos dias"
}
