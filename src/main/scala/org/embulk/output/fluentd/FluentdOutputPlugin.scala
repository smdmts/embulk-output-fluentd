package org.embulk.output.fluentd

import java.util

import org.embulk.config._
import org.embulk.spi._

class FluentdOutputPlugin extends OutputPlugin {

  override def transaction(config: ConfigSource,
                           schema: Schema,
                           taskCount: Int,
                           control: OutputPlugin.Control): ConfigDiff = {
    val task = config.loadConfig(classOf[PluginTask])
    Emitter.init(task)
    control.run(task.dump())
    Emitter.close()
    Exec.newConfigDiff
  }

  override def resume(taskSource: TaskSource,
                      schema: Schema,
                      taskCount: Int,
                      control: OutputPlugin.Control): ConfigDiff = throw new UnsupportedOperationException(
    "fluentd output plugin does not support resuming")

  override def cleanup(taskSource: TaskSource,
                       schema: Schema,
                       taskCount: Int,
                       successTaskReports: util.List[TaskReport]): Unit = {}

  override def open(taskSource: TaskSource,
                    schema: Schema,
                    taskIndex: Int): TransactionalPageOutput =
    FluentdTransactionalPageOutput(taskSource, schema , taskIndex)

}
