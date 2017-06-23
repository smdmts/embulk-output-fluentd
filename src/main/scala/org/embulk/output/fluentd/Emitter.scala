package org.embulk.output.fluentd

import java.time.Instant

import akka.actor._
import akka.stream._
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl._
import akka.util.ByteString
import org.embulk.spi.Exec
import org.velvia.MsgPack

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

object Emitter {

  implicit val system = ActorSystem("fluentd-emitter")
  implicit val m = ActorMaterializer(
    ActorMaterializerSettings(system)
      .withSupervisionStrategy(_ => Supervision.Resume)
      .withDispatcher("blocking-dispatcher"))

  private val logger = Exec.getLogger(getClass)
  private var tag: Option[String] = None
  private var tcp
    : Option[Flow[ByteString, ByteString, Future[OutgoingConnection]]] = None
  private val time = Instant.now().getEpochSecond

  def init(task: PluginTask): Unit = {
    tag = Some(task.getTag)
    tcp = Some(Tcp().outgoingConnection(task.getHost, task.getPort))
  }

  def apply(value: () => Iterator[Map[String, AnyRef]]): Unit =
    Source
      .fromIterator(value)
      .via(converter)
      .buffer(5, OverflowStrategy.backpressure) // TODO change embulk-parameter.
      .throttle(5, 1.seconds, 1, ThrottleMode.shaping)
      .via(tcp.get) // TODO error handling
      .runWith(Sink.ignore)

  private val converter =
    Flow[Map[String, AnyRef]].map { v =>
      ByteString(MsgPack.pack(Seq(tag.get, time, v)))
    }


//  def retry[T](f: => Future[T], delay: FiniteDuration, c: Int)(implicit ec:ExecutionContext): Future[T] =
//    f.recoverWith {
//      case _: Exception if c > 0 =>
//        logger.info(s"failed - will retry ${c - 1} more times")
//        akka.pattern.after(delay, using = syste m.scheduler)(
//          retry(f, delay, c - 1))
//    }

  def close(): Unit = {
    logger.info("closing")
    // wait for akka-stream termination.
    Await.result(system.terminate(), Duration.Inf)
  }

}
