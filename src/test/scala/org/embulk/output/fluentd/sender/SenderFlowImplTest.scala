package org.embulk.output.fluentd.sender

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.{ImplicitSender, TestKit}
import org.velvia.MsgPackUtils._
import org.embulk.output.fluentd.TestActorManager
import org.scalatest._

class SenderFlowImplTest
    extends TestKit(ActorSystem("MySpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "msgPackFlow" should "converting Success" in {
    val target                = SenderFlowImpl("dummy", 123, None)
    val actorManager          = TestActorManager(system)
    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system))
    val map                   = Map[String, AnyRef]("a" -> Int.box(1), "b" -> "e")
    val request               = Seq(Seq(map), Seq(map))

    val (pub, sub) = TestSource
      .probe[Seq[Seq[Map[String, AnyRef]]]]
      .via(target.msgPackFlow)
      .toMat(TestSink.probe[(Int, akka.util.ByteString)])(Keep.both)
      .run()

    sub.request(n = 1)
    pub.sendNext(request)

    val (recordSize, result) = sub.expectNext()

    val that    = unpackSeq(result.toByteBuffer.array())
    val records = that(1).asInstanceOf[Vector[_]]
    recordSize should be(2) // record size
    that.head should be("dummy")
    records.size should be(2) // internal record size
    val one = records(0).asInstanceOf[Vector[_]]
    one(0) should be(123) // time
    one(1).asInstanceOf[Map[_, _]] should be(map)

    val two = records(1).asInstanceOf[Vector[_]]
    two(0) should be(123) // time
    two(1).asInstanceOf[Map[_, _]] should be(map)
  }

  "msgPackFlow" should "converting Success with time key" in {
    val target                = SenderFlowImpl("dummy", 123, Option("time"))
    val actorManager          = TestActorManager(system)
    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system))
    val map                   = Map[String, AnyRef]("a" -> Int.box(1), "b" -> "e", "time" -> Long.box(12345))
    val request               = Seq(Seq(map), Seq(map))

    val (pub, sub) = TestSource
      .probe[Seq[Seq[Map[String, AnyRef]]]]
      .via(target.msgPackFlow)
      .toMat(TestSink.probe[(Int, akka.util.ByteString)])(Keep.both)
      .run()

    sub.request(n = 1)
    pub.sendNext(request)

    val (recordSize, result) = sub.expectNext()

    val that    = unpackSeq(result.toByteBuffer.array())
    val records = that(1).asInstanceOf[Vector[_]]
    recordSize should be(2) // record size
    that.head should be("dummy")
    records.size should be(2) // internal record size
    val one = records(0).asInstanceOf[Vector[_]]
    one(0) should be(12345) // time
    one(1).asInstanceOf[Map[_, _]] should be(map)

    val two = records(1).asInstanceOf[Vector[_]]
    two(0) should be(12345) // time
    two(1).asInstanceOf[Map[_, _]] should be(map)
  }

}
