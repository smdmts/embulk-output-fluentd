package org.embulk.output.fluentd.sender

import java.time.Instant

import org.embulk.output.fluentd.PluginTask
import org.embulk.spi.Exec
import wvlet.airframe.{Design, newDesign}

object SenderBuilder {

  // estimate 27000 records per/send
  val sendingGroupSize = 1000

  def apply(task: PluginTask): Design = {
    implicit val logger = Exec.getLogger(classOf[Sender])
    newDesign
      .bind[SenderFlow]
      .toInstance(SenderFlowImpl(task.getTag, Instant.now().getEpochSecond))
      .bind[ActorManager]
      .toInstance(ActorManager())
      .bind[Sender]
      .toProvider { (senderFlow: SenderFlow, actorManager: ActorManager) =>
        SenderImpl(task.getHost,
                   task.getPort,
                   sendingGroupSize,
                   task.getAsyncSize,
                   senderFlow,
                   actorManager,
                   task.getRequestPerSeconds,
                   retryCount = 10)
      }
  }

}
