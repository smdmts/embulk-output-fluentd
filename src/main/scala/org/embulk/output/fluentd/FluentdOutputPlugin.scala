package org.embulk.output.fluentd

import java.util

import org.embulk.config._
import org.embulk.output.fluentd.sender._
import org.embulk.spi._
import wvlet.log._

class FluentdOutputPlugin extends OutputPlugin {

  override def transaction(config: ConfigSource,
                           schema: Schema,
                           taskCount: Int,
                           control: OutputPlugin.Control): ConfigDiff = {
    Logger.setDefaultLogLevel(LogLevel.OFF)
    val task = config.loadConfig(classOf[PluginTask])
    FluentdOutputPlugin.executeTransaction = true
    FluentdOutputPlugin.taskCountOpt = Some(taskCount)
    control.run(task.dump())
    Exec.newConfigDiff
  }

  override def resume(taskSource: TaskSource,
                      schema: Schema,
                      taskCount: Int,
                      control: OutputPlugin.Control): ConfigDiff =
    throw new UnsupportedOperationException("fluentd output plugin does not support resuming")

  override def cleanup(taskSource: TaskSource,
                       schema: Schema,
                       taskCount: Int,
                       successTaskReports: util.List[TaskReport]): Unit = {}

  override def open(taskSource: TaskSource, schema: Schema, taskIndex: Int): TransactionalPageOutput = {
    FluentdOutputPlugin.sender match {
      case Some(v) =>
        FluentdTransactionalPageOutput(taskSource, schema, taskIndex, v)
      case None => // for map/reduce executor.
        val task = taskSource.loadTask(classOf[PluginTask])
        SenderBuilder(task).withSession { session =>
          val sender = session.build[Sender]
          FluentdOutputPlugin.sender = Option(sender)
          FluentdTransactionalPageOutput(taskSource, schema, taskIndex, sender)
        }
    }
  }

}

object FluentdOutputPlugin {
  var sender: Option[Sender]      = None
  var executeTransaction: Boolean = false
  var taskCountOpt: Option[Int]   = None
}
