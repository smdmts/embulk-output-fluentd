package org.embulk.output.fluentd.sender

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import org.scalatest.{FlatSpec, Matchers}
import org.embulk.output.fluentd.TestActorManager
import org.slf4j.helpers.NOPLogger

import scala.util.{Failure, Success}

class SenderImplTest extends FlatSpec with Matchers {

  implicit val logger = NOPLogger.NOP_LOGGER

  "Sending Success" should "receive dummy server" in {
    val actorManager = TestActorManager()
    bootDummyServer(actorManager.internal.system, "127.0.0.1", actorManager.port)
    Thread.sleep(100) // wait for server boot.
    val sender = SenderImpl(
      "localhost",
      port = actorManager.port,
      groupedSize = 1,
      asyncSize = 1,
      SenderFlowImpl("tag", 0),
      actorManager.internal
    )

    val recode: () => Iterator[Map[String, AnyRef]] =
      () => Seq(Map[String, AnyRef]("a" -> Int.box(1), "b" -> "c")).toIterator
    sender(recode)
    sender.close()

    sender.commands.size shouldBe 1
    sender.commands.forall { v =>
      v.value.get.isSuccess
    } shouldBe true

    sender.recordCount.get() should be(1)
    sender.completedCount.get() should be(1)
    sender.retriedRecordCount.get() should be(0)

    actorManager.internal.terminate()

  }

  def bootDummyServer(system: ActorSystem, address: String, port: Int): Unit = {
    implicit val sys = system
    import system.dispatcher
    implicit val materializer = ActorMaterializer()

    val handler = Sink.foreach[Tcp.IncomingConnection] { conn =>
      println("Client connected from: " + conn.remoteAddress)
      conn handleWith Flow[ByteString]
    }

    val connections = Tcp().bind(address, port)
    val binding     = connections.to(handler).run()

    binding.onComplete {
      case Success(b) =>
        println("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        println(s"Server could not bind to $address:$port: ${e.getMessage}")
        system.terminate()
    }
  }

  "All Failure" should "retry count is correct" in {
    val actorManager    = ActorManager()
    implicit val system = actorManager.system
    implicit val mat    = ActorMaterializer(ActorMaterializerSettings(system).withDebugLogging(true).withFuzzing(true))
    val sender = SenderImpl(
      "localhost",
      port = 9999,
      groupedSize = 1,
      asyncSize = 1,
      SenderFlowImpl("tag", 0),
      actorManager,
      retryCount = 2, // 2 times
      retryDelayIntervalSecond = 1 // 1 seconds
    )

    val recode: () => Iterator[Map[String, AnyRef]] =
      () => Seq(Map[String, AnyRef]("a" -> Int.box(1), "b" -> "c")).toIterator
    sender(recode)
    sender.close()

    sender.commands.size shouldBe 3 // original + 2 times retry.
    sender.commands.forall { v =>
      v.value.get.isFailure // all failure
    } shouldBe true

    sender.recordCount.get() should be(1)
    sender.retriedRecordCount.get() should be(2)
    sender.completedCount.get() should be(0)

    actorManager.terminate()
  }

}
