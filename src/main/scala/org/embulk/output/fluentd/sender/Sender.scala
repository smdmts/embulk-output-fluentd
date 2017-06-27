package org.embulk.output.fluentd.sender

import akka._
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import org.embulk.spi.Exec

import scala.collection.mutable.ListBuffer
import scala.concurrent._
import scala.concurrent.duration._
import scala.util._

trait Sender {
  def close(): Unit
  val instance: SourceQueueWithComplete[Seq[Map[String, AnyRef]]]
  def apply(value: () => Iterator[Map[String, AnyRef]]): Future[QueueOfferResult]
  def tcpHandling(size: Int, byteString: ByteString): Future[Done]
}

case class SenderImpl(host: String,
                      port: Int,
                      groupedSize: Int,
                      asyncSize: Int,
                      senderFlow: SenderFlow,
                      actorManager: ActorManager)
    extends Sender {
  import actorManager._
  private val logger  = Exec.getLogger(getClass)
  private val futures = ListBuffer.empty[Future[akka.Done]]

  def apply(value: () => Iterator[Map[String, AnyRef]]): Future[QueueOfferResult] =
    instance.offer(value().toSeq)

  def close(): Unit = {
    instance.complete()
    // wait for akka-stream termination.
    Await.result(instance.watchCompletion(), Duration.Inf)
    Await.result(Future.sequence(futures), Duration.Inf)
    Await.result(actorManager.terminate(), Duration.Inf)
    logger.info("closing")
  }

  val instance: SourceQueueWithComplete[Seq[Map[String, AnyRef]]] = Source
    .queue(Int.MaxValue, OverflowStrategy.backpressure)
    .grouped(groupedSize)
    .via(senderFlow.msgPackFlow)
    //.throttle(asyncSize, 1.seconds, 0, ThrottleMode.Shaping)
    .mapAsync(asyncSize) {
      case (size, byteString) =>
        tcpHandling(size, byteString)
    }
    .to(Sink.ignore)
    .run()

  def tcpHandling(size: Int, byteString: ByteString): Future[Done] = {
    logger.info(s"Sending fluentd to ${size.toString} records.")
    val f = () =>
      Source
        .single(byteString)
        .via(senderFlow.tcpConnectionFlow(host, port))
        .runWith(Sink.ignore)
    retry(f, size, 10.seconds, 10)
  }

  def retry(func: () => Future[Done], size: Int, delay: FiniteDuration, c: Int): Future[Done] = {
    val future = func()
    futures += future
    future.onComplete {
      case Success(_) =>
        logger.info(s"Sending fluentd to ${size.toString} records was completed.")
      case Failure(e) =>
        throw e
    }
    future.recoverWith {
      case e: Exception if c > 0 =>
        logger.error(
          s"Sending fluentd ${size.toString} records was failed. - will retry ${c - 1} more times ${delay.toSeconds} seconds later.",
          e)
        akka.pattern.after(delay, using = system.scheduler)(retry(func, size, delay, c - 1))
      case e: Exception if c == 0 =>
        logger.error(
          s"Sending fluentd retry count is over and will be terminate soon. Please check your fluentd environment.",
          e)
        system.terminate()
        sys.error("Actor system was terminated cause of retry count over.")
    }
  }
}
