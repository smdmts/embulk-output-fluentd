package org.embulk.output.fluentd

import java.io.IOException
import java.net.ServerSocket
import java.util.concurrent.ThreadLocalRandom

import akka.stream.scaladsl.{Source, Tcp}
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import org.embulk.output.fluentd.sender.ActorManager

import scala.concurrent.Future

object TestActorManager {

  val actorManager = ActorManager()

  val port:Int = TestActorManager.freePort(8888, 8999)

  val dummyServer: Source[IncomingConnection, Future[ServerBinding]] =
    Tcp().bind("127.0.0.1", port)

  def freePort(from: Int, to: Int): Int = {
    var port = from
    while(true) {
      if (isLocalPortFree(port)) return port
      else port = ThreadLocalRandom.current.nextInt(from, to)
    }
    port
  }

  private def isLocalPortFree(port: Int) = try {
    new ServerSocket(port).close()
    true
  } catch {
    case _: IOException =>
      false
  }
}
