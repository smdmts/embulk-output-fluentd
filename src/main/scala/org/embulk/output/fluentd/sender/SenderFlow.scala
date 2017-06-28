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
  def tcpConnectionFlow(host: String, port: Int)(
      implicit s: ActorSystem): Flow[ByteString, ByteString, Future[OutgoingConnection]]
}

case class SenderFlowImpl private[sender] (tag: String, unixtime: Long) extends SenderFlow {
  override val msgPackFlow: Flow[Seq[Seq[Map[String, AnyRef]]], (Int, ByteString), NotUsed] =
    Flow[Seq[Seq[Map[String, AnyRef]]]].map { value =>
      val packing = value.flatten.map { v =>
        Seq(unixtime, v)
      }
      (packing.size, ByteString(MsgPack.pack(Seq(tag, packing))))
    }
  override def tcpConnectionFlow(host: String, port: Int)(
      implicit s: ActorSystem): Flow[ByteString, ByteString, Future[OutgoingConnection]] =
    Tcp().outgoingConnection(host, port)

}
