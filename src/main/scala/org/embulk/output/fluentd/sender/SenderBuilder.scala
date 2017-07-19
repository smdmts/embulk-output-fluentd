package org.embulk.output.fluentd.sender

import java.time.Instant

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.embulk.output.fluentd.PluginTask
import org.embulk.spi.Exec
import wvlet.airframe.{Design, newDesign}

object SenderBuilder {

  def apply(task: PluginTask): Design = {
    implicit val logger = Exec.getLogger(classOf[Sender])
    implicit val system = ActorSystem("fluentd-sender",
                                      ConfigFactory.load(this.getClass.getClassLoader, "application.conf"),
                                      this.getClass.getClassLoader)
    newDesign
      .bind[SenderFlow]
      .toInstance(SenderFlowImpl(task.getTag, Instant.now().getEpochSecond, Option(task.getTimeKey)))
      .bind[ActorManager]
      .toInstance(ActorManagerImpl())
      .bind[Sender]
      .toProvider { (senderFlow: SenderFlow, actorManager: ActorManager) =>
        SenderImpl(task.getHost,
                   task.getPort,
                   task.getRequestGroupingSize,
                   task.getAsyncSize,
                   senderFlow,
                   actorManager,
                   task.getRequestPerSeconds,
                   retryCount = 10)
      }
  }

}
