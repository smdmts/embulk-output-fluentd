package org.embulk.output.fluentd.sender

import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import org.velvia.MsgPackUtils._

import org.embulk.output.fluentd.TestActorManager
import org.scalatest.{FlatSpec, Matchers}

class SenderFlowImplTest extends FlatSpec with Matchers {

  val target = SenderFlowImpl("dummy", 123)

  "msgPackFlow" should "converting Success" in {
    val actorManager = TestActorManager()
    implicit val s   = actorManager.internal.system
    implicit val m   = actorManager.internal.materializer
    val map          = Map[String, AnyRef]("a" -> Int.box(1), "b" -> "e")
    val request      = Seq(Seq(map), Seq(map))

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

}
