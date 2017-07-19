package org.embulk.output.fluentd.sender

import akka.actor._
import org.slf4j.Logger

class SuperVisor extends Actor {
  var complete = 0
  var failed   = 0
  var retried  = 0
  var counter  = 0
  var closed   = false
  override def receive: Receive = {
    case Record(v) =>
      counter = counter + v
    case Complete(v) =>
      complete = complete + v
    case Failed(v)  => failed = failed + v
    case Retried(v) => retried = retried + v
    case GetStatus =>
      if (failed == 0) {
        sender() ! Result(counter, complete, failed, retried)
      } else {
        sender() ! Stop(counter, complete, failed, retried)
      }
    case Close =>
      val result = ClosedStatus(closed)
      if (!closed) {
        closed = true
      }
      sender() ! result
    case LogStatus(logger) =>
      logger.info(
        s"$counter was queued and $complete records was completed. $failed records was failed and retried $retried records.")
  }
}

case class Result(record: Int, complete: Int, failed: Int, retried: Int)
case object GetStatus
case class Stop(record: Int, complete: Int, failed: Int, retried: Int)
case object Close
case class ClosedStatus(alreadyClosed: Boolean)
case class LogStatus(logger: Logger)
case class Record(count: Int)   extends AnyVal
case class Complete(count: Int) extends AnyVal
case class Failed(count: Int)   extends AnyVal
case class Retried(count: Int)  extends AnyVal
