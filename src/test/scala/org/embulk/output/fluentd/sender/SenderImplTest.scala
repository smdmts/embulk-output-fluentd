package org.embulk.output.fluentd.sender

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.embulk.output.fluentd.TestActorManager
import org.slf4j.helpers.NOPLogger

import scala.concurrent.duration._

class SenderImplTest extends FlatSpecLike with Matchers with BeforeAndAfterAll {

  implicit val logger  = NOPLogger.NOP_LOGGER
  implicit val timeout = Timeout(5.seconds)

  "Sending Success fast server" should "receive dummy server" in {

    val system       = ActorSystem("MySpec")
    val actorManager = TestActorManager(system)
    bootDummyServer(system, "127.0.0.1", actorManager.port, Duration.create(0, TimeUnit.SECONDS))
    Thread.sleep(100) // wait for server boot.
    val sender = SenderImpl(
      "localhost",
      port = actorManager.port,
      groupedSize = 1,
      asyncSize = 1,
      SenderFlowImpl("tag", 0),
      actorManager
    )

    (1 to 100).foreach { _ =>
      sender(recode)
    }

    sender.waitForComplete()

    actorManager.testActorRef.underlyingActor.counter should be(100)
    actorManager.testActorRef.underlyingActor.complete should be(100)
    actorManager.testActorRef.underlyingActor.retried should be(0)

    actorManager.system.terminate()

  }

  "Sending Success slow server" should "receive dummy server" in {

    val system       = ActorSystem("MySpec")
    val actorManager = TestActorManager(system)
    bootDummyServer(system, "127.0.0.1", actorManager.port, Duration.create(3, TimeUnit.SECONDS))
    Thread.sleep(100) // wait for server boot.
    val sender = SenderImpl(
      "localhost",
      port = actorManager.port,
      groupedSize = 1,
      asyncSize = 1,
      SenderFlowImpl("tag", 0),
      actorManager
    )

    (1 to 2).foreach { _ =>
      sender(recode)
    }

    sender.waitForComplete()

    actorManager.testActorRef.underlyingActor.counter should be(2)
    actorManager.testActorRef.underlyingActor.complete should be(2)
    actorManager.testActorRef.underlyingActor.retried should be(0)

    actorManager.system.terminate()
  }

  def recode: () => Iterator[Map[String, AnyRef]] =
    () => Seq(Map[String, AnyRef]("a" -> Int.box(1), "b" -> "c")).toIterator

  def bootDummyServer(system: ActorSystem, address: String, port: Int, duration: Duration): Unit = {
    implicit val sys          = system
    implicit val materializer = ActorMaterializer()
    val connections           = Tcp().bind(address, port)
    connections runForeach { connection =>
      val echo = Flow[ByteString]
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 256, allowTruncation = true))
        .map { v =>
          TimeUnit.SECONDS.sleep(duration.toSeconds)
          ByteString(v.utf8String)
        }
      connection.handleWith(echo)
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
      SenderFlowImpl("tag", 0),
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

    actorManager.system.terminate()
  }

}
