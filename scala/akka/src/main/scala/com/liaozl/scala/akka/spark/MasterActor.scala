package com.liaozl.scala.akka.spark

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

/**
  * @Description:
  * @version: v1.0.0
  * @author: liaozuliang
  * @date: 2020/9/10 11:52
  */
class MasterActor extends Actor {

  var worksMap = collection.mutable.HashMap[String, WorkInfo]()

  override def receive: Receive = {
    case "start" => {
      println("Master启动成功")

      import context.dispatcher
      context.system.scheduler.schedule(0 millis, 9000 millis, self, CountWorks)
    }
    case RegisterInfo(workId, workHost, workPort) => {
      if (!worksMap.contains(workId)) {
        worksMap += (workId -> new WorkInfo(workId, workHost, workPort))
        println(s"Work(Id:${workId})注册成功")
        sender() ! RegisterSuccess
      }
    }
    case HeartBeatInfo(workId, workHost, workPort) => {
      if (!worksMap.contains(workId)) {
        worksMap += (workId -> new WorkInfo(workId, workHost, workPort))
      }

      worksMap.get(workId).get.lastHeartBeat = System.currentTimeMillis()

      sender() ! HeartBeatSuccess
    }
    case CountWorks => {
      worksMap.values.filter(w => System.currentTimeMillis() - w.lastHeartBeat > 6000).foreach(w => worksMap.remove(w.id))

      println("=============当前有效的work信息如下=========")
      for (w <- worksMap.values) {
        println(w)
      }
    }
  }
}

object MasterActorApp {

  def main(args: Array[String]): Unit = {
    var host = "127.0.0.1"
    var port = 10001

    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname=$host
         |akka.remote.netty.tcp.port=$port
         """.stripMargin)

    val actorSystem = ActorSystem("masterActorSystem", config)

    val masterActorRef = actorSystem.actorOf(Props[MasterActor], "masterActor")

    masterActorRef ! "start"

  }

}
