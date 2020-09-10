package com.liaozl.scala.akka.spark


case class BeginRegister()

case class RegisterInfo(id: String, host: String, port: Int)

case class RegisterSuccess()

case class BeginHeartBeat()

case class HeartBeatInfo(id: String, host: String, port: Int)

case class HeartBeatSuccess()

case class CountWorks()

class WorkInfo(var id: String, var host: String, var port: Int) {
  var lastHeartBeat: Long = System.currentTimeMillis()

  override def toString: String = {
    s"id:${id}, host:${host}, port:${port}"
  }
}