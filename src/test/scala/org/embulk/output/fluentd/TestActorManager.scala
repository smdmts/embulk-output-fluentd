package org.embulk.output.fluentd

import java.io.IOException
import java.net.ServerSocket
import java.util.concurrent.ThreadLocalRandom

import org.embulk.output.fluentd.sender.ActorManager

case class TestActorManager() {
  val internal     = ActorManager()
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
}
