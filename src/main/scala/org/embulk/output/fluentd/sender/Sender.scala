package org.embulk.output.fluentd.sender

import akka._
import akka.pattern.ask
import akka.stream._
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}
import org.slf4j.Logger

import scala.concurrent._
import scala.concurrent.duration._
import scala.util._

trait Sender {
  def close(): Unit
  val instance: SourceQueueWithComplete[Seq[Map[String, AnyRef]]]
  def apply(value: () => Iterator[Map[String, AnyRef]]): Future[QueueOfferResult]
  def tcpHandling(size: Int, byteString: ByteString): Future[Done]
  def waitForComplete(): Result
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

  system.scheduler.schedule(0.seconds, 30.seconds, supervisor, LogStatus(logger))

  val retryDelayIntervalSecondDuration: FiniteDuration = retryDelayIntervalSecond.seconds

  def apply(value: () => Iterator[Map[String, AnyRef]]): Future[QueueOfferResult] = {
    val request = value().toSeq
    actorManager.supervisor ! Record(request.size)
    instance.offer(request)
  }

  def close(): Unit = {
    // wait for akka-stream termination.
    instance.complete()
    val result = waitForComplete()
    Await.result(actorManager.terminate(), Duration.Inf)
    actorManager.system.terminate()
    logger.info(
      s"Transaction was closed. recordCount:${result.record} completedCount:${result.complete} retriedRecordCount:${result.retried}")
  }

  def waitForComplete(): Result = {
    var result: Option[Result] = None
    implicit val timeout       = Timeout(5.seconds)
    while (result.isEmpty) {
      (actorManager.supervisor ? GetStatus).onSuccess {
        case Result(recordCount, complete, failed, retried) =>
          if (recordCount == (complete + failed)) {
            result = Some(Result(recordCount, complete, failed, retried))
          }
        case Stop(recordCount, complete, failed, retried) =>
          result = Some(Result(recordCount, complete, failed, retried))
      }
      Thread.sleep(1000)
    }
    result.get
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
    def _tcpHandling(size: Int, byteString: ByteString, c: Int)(retried: Boolean): Future[Done] = {
      val futureCommand = sendCommand(byteString)
      futureCommand.onComplete {
        case Success(_) =>
          actorManager.supervisor ! Complete(size)
        case Failure(e) if c > 0 =>
          logger.info(
            s"Sending fluentd ${size.toString} records was failed. - will retry ${c - 1} more times ${retryDelayIntervalSecondDuration.toSeconds} seconds later.",
            e)
          actorManager.supervisor ! Retried(size)
          akka.pattern.after(retryDelayIntervalSecondDuration, actorManager.system.scheduler)(
            _tcpHandling(size, byteString, c - 1)(retried = true))
        case Failure(e) =>
          actorManager.supervisor ! Failed(size)
          logger.error(
            s"Sending fluentd retry count is over and will be terminate soon. Please check your fluentd environment.",
            e)
          sys.error("Sending fluentd was terminated cause of retry count over.")
          instance.complete()
      }
      futureCommand
    }
    _tcpHandling(size, byteString, retryCount)(retried = false).recoverWith {
      case _: Exception =>
        Future.successful(Done)
    }
  }

}
