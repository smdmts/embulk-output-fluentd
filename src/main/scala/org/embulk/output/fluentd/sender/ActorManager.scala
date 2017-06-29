package org.embulk.output.fluentd.sender

import akka.actor.{ActorSystem, Terminated}
import akka.dispatch.MessageDispatcher
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}

import scala.concurrent.Future

case class ActorManager() {

  implicit val system = ActorSystem("fluentd-sender")

  val decider: Supervision.Decider = {
    case _: Exception => Supervision.Resume
    case _            => Supervision.Stop
  }

  implicit val materializer = ActorMaterializer(
    ActorMaterializerSettings(system)
      .withSupervisionStrategy(decider)
      .withDispatcher("blocking-dispatcher"))

  implicit val dispatcher: MessageDispatcher =
    system.dispatchers.lookup("blocking-dispatcher")

  def terminate(): Future[Terminated] = system.terminate()

}
