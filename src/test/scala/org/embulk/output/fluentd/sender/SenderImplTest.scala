package org.embulk.output.fluentd.sender

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.embulk.output.fluentd.TestActorManager
import org.slf4j.helpers.NOPLogger

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}
import scala.concurrent.duration._

class SenderImplTest extends FlatSpecLike with Matchers with BeforeAndAfterAll {

  implicit val logger  = NOPLogger.NOP_LOGGER
  implicit val timeout = Timeout(5.seconds)

  "Sending Success" should "receive dummy server" in {

    val system       = ActorSystem("MySpec")
    val actorManager = TestActorManager(system)
    bootDummyServer(system, "127.0.0.1", actorManager.port)
    Thread.sleep(100) // wait for server boot.
    val sender = SenderImpl(
      "localhost",
      port = actorManager.port,
      groupedSize = 1,
      asyncSize = 1,
      SenderFlowImpl("tag", 0, None),
      actorManager
    )

    val recode: () => Iterator[Map[String, AnyRef]] =
      () => Seq(Map[String, AnyRef]("a" -> Int.box(1), "b" -> "c")).toIterator
    sender(recode)
    sender.waitForComplete()

    actorManager.testActorRef.underlyingActor.counter should be(1)
    actorManager.testActorRef.underlyingActor.complete should be(1)
    actorManager.testActorRef.underlyingActor.retried should be(0)

  }

  def bootDummyServer(system: ActorSystem, address: String, port: Int): Unit = {
    implicit val ec = ExecutionContext.global
    implicit val s  = system
    implicit val m  = ActorMaterializer()

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
    }
  }

  "All Failure" should "retry count is correct" in {
    val system       = ActorSystem("MySpec")
    val actorManager = TestActorManager(system)
    val sender = SenderImpl(
      "localhost",
      port = 9999,
      groupedSize = 1,
      asyncSize = 1,
      SenderFlowImpl("tag", 0, None),
      actorManager,
      retryCount = 2, // 2 times
      retryDelayIntervalSecond = 1 // 1 seconds
    )

    val recode: () => Iterator[Map[String, AnyRef]] =
      () => Seq(Map[String, AnyRef]("a" -> Int.box(1), "b" -> "c")).toIterator
    sender(recode)
    sender.waitForComplete()

    actorManager.testActorRef.underlyingActor.counter should be(1)
    actorManager.testActorRef.underlyingActor.retried should be(2)
    actorManager.testActorRef.underlyingActor.complete should be(0)

  }

}
