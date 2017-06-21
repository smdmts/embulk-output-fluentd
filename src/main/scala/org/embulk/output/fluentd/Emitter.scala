package org.embulk.output.fluentd

import org.komamitsu.fluency.Fluency

import scala.collection.JavaConverters._
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._

import scala.concurrent.duration._
import org.embulk.spi.Exec

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

object Emitter {

  implicit val system = ActorSystem("fluentd-emitter")
  implicit val m = ActorMaterializer(
    ActorMaterializerSettings(system).withSupervisionStrategy(_ =>
      Supervision.Resume))

  private val logger = Exec.getLogger(getClass)
  private var emitter: Option[Fluency] = None
  private var tag: Option[String] = None

  def init(task: PluginTask): Unit = {
    val config = new Fluency.Config()
    emitter = Some(
      Fluency.defaultFluency(
        task.getHost,
        task.getPort,
        config
      ))
    tag = Some(task.getTag)
  }

  def apply(value: () => Iterator[Map[String, AnyRef]]): Unit = {
    Source
      .fromIterator(value)
      .via(emitFlow)
      .runWith(Sink.ignore)
  }

  private val emitFlow =
    Flow[Map[String, AnyRef]]
      .buffer(1, OverflowStrategy.backpressure)
      .via {
        Flow[Map[String, AnyRef]]
          .throttle(1, 30.seconds, 1, ThrottleMode.shaping)
          .mapAsync(1) { v =>
            val future = Future {
              emitter.foreach(_.emit(tag.get, v.asJava))
            }
            retry(future, 1 second, 10)
          }
      }

  def retry[T](f: => Future[T], delay: FiniteDuration, c: Int): Future[T] =
    f.recoverWith {
      case _: Exception if c > 0 =>
        logger.info(s"failed - will retry ${c - 1} more times")
        akka.pattern.after(delay, using = system.scheduler)(
          retry(f, delay, c - 1))
    }

  def close(): Unit = {
    emitter.foreach { v =>
      v.close()
    }

    logger.info("closing")
    // wait for akka-stream termination.
    Await.result(system.terminate(), Duration.Inf)
  }

}
