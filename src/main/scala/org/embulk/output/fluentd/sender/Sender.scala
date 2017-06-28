package org.embulk.output.fluentd.sender

import akka._
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import org.slf4j.Logger

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

case class SenderImpl private[sender] (host: String,
                                       port: Int,
                                       groupedSize: Int,
                                       asyncSize: Int,
                                       senderFlow: SenderFlow,
                                       actorManager: ActorManager,
                                       asyncSizeRequestPerSecond: Int = 0,
                                       retryCount: Int = 0,
                                       retryDelayIntervalSecond: Int = 10)(implicit logger: Logger)
    extends Sender {
  import actorManager._
  private[sender] val futures = ListBuffer.empty[Future[akka.Done]]

  val retryDelayIntervalSecondDuration: FiniteDuration = retryDelayIntervalSecond.seconds

  def apply(value: () => Iterator[Map[String, AnyRef]]): Future[QueueOfferResult] =
    instance.offer(value().toSeq)

  def close(): Unit = {
    instance.complete() // wait for akka-stream termination.
    Await.result(instance.watchCompletion(), Duration.Inf)
    while (!futures.forall(_.isCompleted)) {
      Await.ready(Future.sequence(futures), Duration.Inf)
      Thread.sleep(1000)
    }
    Await.result(actorManager.terminate(), Duration.Inf)
    logger.info("Transaction was closed.")
  }

  val instance: SourceQueueWithComplete[Seq[Map[String, AnyRef]]] = {
    val base = Source
      .queue(Int.MaxValue, OverflowStrategy.backpressure)
      .grouped(groupedSize)
      .via(senderFlow.msgPackFlow)
    val withThrottle = if (asyncSizeRequestPerSecond > 0) {
      base.throttle(asyncSize, asyncSizeRequestPerSecond.seconds, 0, ThrottleMode.Shaping)
    } else base
    withThrottle
      .mapAsync(asyncSize) {
        case (size, byteString) =>
          tcpHandling(size, byteString)
      }
      .to(Sink.ignore)
      .run()
  }

  def tcpHandling(size: Int, byteString: ByteString): Future[Done] = {
    logger.info(s"Sending fluentd to ${size.toString} records.")
    def _tcpHandling(size: Int, byteString: ByteString, c: Int)(first: Boolean): Future[Done] = {
      val future = Source
        .single(byteString)
        .via(senderFlow.tcpConnectionFlow(host, port))
        .runWith(Sink.ignore)
      if (first) {
        futures += future
      }
      future.onComplete {
        case Success(_) =>
          logger.info(s"Sending fluentd to ${size.toString} records was completed.")
        case Failure(e) if c > 0 =>
          logger.info(
            s"Sending fluentd ${size.toString} records was failed. - will retry ${c - 1} more times ${retryDelayIntervalSecondDuration.toSeconds} seconds later.",
            e)
          val retried =
            akka.pattern.after(retryDelayIntervalSecondDuration, system.scheduler)(
              _tcpHandling(size, byteString, c - 1)(first = false))
          futures += retried
        case Failure(e) if c == 0 =>
          logger.error(
            s"Sending fluentd retry count is over and will be terminate soon. Please check your fluentd environment.",
            e)
          system.terminate()
          sys.error("Sending fluentd was terminated cause of retry count over.")
      }
      future
    }
    val future = _tcpHandling(size, byteString, retryCount)(first = true)
    future recoverWith {
      case _:Exception =>
        Future.successful(Done)
    }
  }

}
