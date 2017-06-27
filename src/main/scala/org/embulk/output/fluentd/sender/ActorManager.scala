package org.embulk.output.fluentd.sender

import akka.actor.{ActorSystem, Terminated}
import akka.dispatch.MessageDispatcher
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}

import scala.concurrent.Future

case class ActorManager() {

  implicit val system = ActorSystem("fluentd-emitter")
  implicit val materializer = ActorMaterializer(
    ActorMaterializerSettings(system)
      .withSupervisionStrategy(_ => Supervision.Stop)
      .withDispatcher("blocking-dispatcher"))

  implicit val dispatcher: MessageDispatcher =
    system.dispatchers.lookup("blocking-dispatcher")

  def terminate(): Future[Terminated] = system.terminate()

}
