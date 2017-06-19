package org.embulk.output.fluentd

import org.embulk.config.{TaskReport, TaskSource}
import org.embulk.spi._

case class FluentdTransactionalPageOutput(taskSource: TaskSource,
                                          schema: Schema,
                                          taskIndex: Int)
    extends TransactionalPageOutput {

  val task: PluginTask = taskSource.loadTask(classOf[PluginTask])
  val reader:PageReader = new PageReader(schema)

  override def add(page: Page): Unit = {
    reader.setPage(page)
    while (reader.nextRecord()) {
      val visitor = ColumnVisitor(reader)
      schema.visitColumns(visitor)
      Emitter.emit(visitor.getRecord)
    }
  }

  override def commit(): TaskReport = Exec.newTaskReport

  override def abort(): Unit = ()
  override def finish(): Unit = ()
  override def close(): Unit =  {
    reader.close()
  }

}
