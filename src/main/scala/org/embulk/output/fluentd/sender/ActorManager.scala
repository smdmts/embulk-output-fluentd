package org.embulk.output.fluentd.sender

import akka.actor.{ActorRef, ActorSystem, Props, Terminated}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}

import scala.concurrent.{ExecutionContext, Future}

case class ActorManagerImpl(implicit val system: ActorSystem) extends ActorManager {
  val supervisor: ActorRef = system.actorOf(Props[Counter])
  val decider: Supervision.Decider = {
    case _: Exception => Supervision.Resume
    case _            => Supervision.Stop
  }
  implicit val materializer = ActorMaterializer(
    ActorMaterializerSettings(system)
      .withSupervisionStrategy(decider)
      .withDispatcher("blocking-dispatcher"))

  implicit val dispatcher: ExecutionContext =
    system.dispatchers.lookup("blocking-dispatcher")
}

trait ActorManager {
  implicit val system: ActorSystem
  val supervisor: ActorRef
  implicit val materializer: ActorMaterializer
  def terminate(): Future[Terminated] = system.terminate()
  implicit val dispatcher: ExecutionContext
}
