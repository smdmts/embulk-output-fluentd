package org.embulk.output.fluentd.sender

import java.util.concurrent.atomic.AtomicInteger

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
  private[sender] val commands = ListBuffer.empty[Future[akka.Done]]

  val recordCount        = new AtomicInteger(0)
  val retriedRecordCount = new AtomicInteger(0)
  val completedCount     = new AtomicInteger(0)

  val retryDelayIntervalSecondDuration: FiniteDuration = retryDelayIntervalSecond.seconds

  def apply(value: () => Iterator[Map[String, AnyRef]]): Future[QueueOfferResult] =
    instance.offer(value().toSeq)

  def close(): Unit = {
    // wait for akka-stream termination.
    instance.complete()
    Await.result(instance.watchCompletion(), Duration.Inf)
    // wait for all commands completed.
    while (!commands.forall(_.isCompleted)) {
      Await.ready(Future.sequence(commands), Duration.Inf)
      Thread.sleep(1000)
    }
    Await.result(actorManager.terminate(), Duration.Inf)
    logger.info(
      s"Transaction was closed. recordCount:$recordCount completedCount:$completedCount retriedRecordCount:$retriedRecordCount")
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

  def sendCommand(byteString: ByteString): Future[Done] =
    Source
      .single(byteString)
      .via(senderFlow.tcpConnectionFlow(host, port))
      .runWith(Sink.ignore)

  def tcpHandling(size: Int, byteString: ByteString): Future[Done] = {
    logger.info(s"Sending fluentd to ${size.toString} records.")
    recordCount.getAndAdd(size)
    def _tcpHandling(size: Int, byteString: ByteString, c: Int)(retried: Boolean): Future[Done] = {
      val futureCommand = sendCommand(byteString)
      futureCommand.onComplete {
        case Success(_) =>
          completedCount.addAndGet(size)
          logger.info(s"Sending fluentd to ${size.toString} records was completed.")
        case Failure(e) if c > 0 =>
          logger.info(
            s"Sending fluentd ${size.toString} records was failed. - will retry ${c - 1} more times ${retryDelayIntervalSecondDuration.toSeconds} seconds later.",
            e)
          retriedRecordCount.addAndGet(size)
          commands += akka.pattern.after(retryDelayIntervalSecondDuration, system.scheduler)(
            _tcpHandling(size, byteString, c - 1)(retried = true))
        case Failure(e) =>
          logger.error(
            s"Sending fluentd retry count is over and will be terminate soon. Please check your fluentd environment.",
            e)
          system.terminate()
          sys.error("Sending fluentd was terminated cause of retry count over.")
      }
      if (!retried) {
        commands += futureCommand
      }
      futureCommand
    }
    _tcpHandling(size, byteString, retryCount)(retried = false).recoverWith {
      case _: Exception =>
        Future.successful(Done)
    }
  }

}
