package org.embulk.output.fluentd

import java.io.IOException
import java.net.ServerSocket
import java.util.concurrent.ThreadLocalRandom

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import akka.testkit.TestActorRef
import org.embulk.output.fluentd.sender._

import scala.concurrent.ExecutionContext

case class TestActorManager(s: ActorSystem) extends ActorManager {
  implicit val system = s
  val port: Int    = freePort(8888, 8999)
  val host: String = "127.0.0.1"

  def freePort(from: Int, to: Int): Int = {
    var port = from
    while (true) {
      if (isLocalPortFree(port)) return port
      else port = ThreadLocalRandom.current.nextInt(from, to)
    }
    port
  }

  private def isLocalPortFree(port: Int) =
    try {
      new ServerSocket(port).close()
      true
    } catch {
      case _: IOException =>
        false
    }

  val testActorRef =  TestActorRef(new Counter)

  override val supervisor: ActorRef = testActorRef
  override implicit val materializer: ActorMaterializer = ActorMaterializer()
  override implicit val dispatcher: ExecutionContext = ExecutionContext.global
}


