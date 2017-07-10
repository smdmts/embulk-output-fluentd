package org.embulk.output.fluentd.sender

import akka.actor.{ActorRef, ActorSystem, Props, Terminated}
import akka.dispatch.MessageDispatcher
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}

import scala.concurrent.Future

case class ActorManager(implicit actorSystem:ActorSystem) {
  val supervisor: ActorRef = actorSystem.actorOf(Props[Counter])
  val decider: Supervision.Decider = {
    case _: Exception => Supervision.Resume
    case _            => Supervision.Stop
  }

  implicit val materializer = ActorMaterializer(
    ActorMaterializerSettings(actorSystem)
      .withSupervisionStrategy(decider)
      .withDispatcher("blocking-dispatcher"))

  implicit val dispatcher: MessageDispatcher =
    actorSystem.dispatchers.lookup("blocking-dispatcher")

  def terminate(): Future[Terminated] = actorSystem.terminate()

}
