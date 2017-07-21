package org.embulk.output.fluentd.sender

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Tcp}
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.util.ByteString
import org.velvia.MsgPack

import scala.concurrent.Future

trait SenderFlow {
  val msgPackFlow: Flow[Seq[Seq[Map[String, AnyRef]]], (Int, ByteString), NotUsed]
}

case class SenderFlowImpl private[sender] (tag: String, unixtime: Long, timeKeyOpt: Option[String])
    extends SenderFlow {
  override val msgPackFlow: Flow[Seq[Seq[Map[String, AnyRef]]], (Int, ByteString), NotUsed] =
    Flow[Seq[Seq[Map[String, AnyRef]]]].map { value =>
      val packing = value.flatten.map { v =>
        val eventTime = for {
          timeKey   <- timeKeyOpt
          timeValue <- v.get(timeKey)
        } yield timeValue.toString.toLong
        val logTime = eventTime.getOrElse(unixtime)
        Seq(logTime, v)
      }
      (packing.size, ByteString(MsgPack.pack(Seq(tag, packing))))
    }
}
