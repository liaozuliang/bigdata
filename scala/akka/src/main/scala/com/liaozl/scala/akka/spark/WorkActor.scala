package com.liaozl.scala.akka.spark


import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.util.Random


/**
  * @Description:
  * @version: v1.0.0
  * @author: liaozuliang
  * @date: 2020/9/10 11:52
  */
class WorkActor(masterHost: String, masterPort: Int, workHost: String, workPort: Int) extends Actor {

  var id: String = java.util.UUID.randomUUID().toString;
  var cpu: Int = 2

  var masterActorProxy: ActorSelection = _

  override def preStart(): Unit = {
    masterActorProxy = context.actorSelection(s"akka.tcp://masterActorSystem@${masterHost}:${masterPort}/user/masterActor")
  }

  override def receive: Receive = {
    case "start" => {
      println("Work启动成功, 开始向Master注册")
      self ! BeginRegister
    }
    case BeginRegister => masterActorProxy ! RegisterInfo(id, workHost, workPort)
    case RegisterSuccess => {
      println("Work注册成功，启动心跳发送")

      import context.dispatcher
      context.system.scheduler.schedule(0 millis, 3000 millis, self, BeginHeartBeat)
    }
    case BeginHeartBeat => {
      masterActorProxy ! HeartBeatInfo(id, workHost, workPort)
    }
    case HeartBeatSuccess => {
      println("发送心跳成功")
    }


  }
}

object WorkActorApp {

  def main(args: Array[String]): Unit = {

    val workHost = "127.0.0.1"
    val workPort = 10002 + new Random().nextInt(500) + 1

    val masterHost = "127.0.0.1"
    val masterPort = 10001

    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname=$workHost
         |akka.remote.netty.tcp.port=$workPort
         """.stripMargin)

    val workActorSystem = ActorSystem("workActorSystem", config)

    val workActorRef = workActorSystem.actorOf(Props(new WorkActor(masterHost, masterPort, workHost, workPort)), "workActor")

    workActorRef ! "start"

  }

}
