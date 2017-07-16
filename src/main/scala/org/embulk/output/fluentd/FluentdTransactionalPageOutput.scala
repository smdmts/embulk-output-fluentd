package org.embulk.output.fluentd

import com.google.common.base.Optional
import org.embulk.config.{TaskReport, TaskSource}
import org.embulk.output.fluentd.sender.Sender
import org.embulk.spi._
import org.embulk.spi.time.TimestampFormatter

case class FluentdTransactionalPageOutput(taskSource: TaskSource, schema: Schema, taskIndex: Int, sender: Sender)
    extends TransactionalPageOutput {

  val task: PluginTask = taskSource.loadTask(classOf[PluginTask])

  def timestampFormatter(): TimestampFormatter =
    new TimestampFormatter(task, Optional.absent())

  override def add(page: Page): Unit = {
    sender(() => asIterator(page))
  }

  def asIterator(page: Page): Iterator[Map[String, AnyRef]] = {
    val reader: PageReader = new PageReader(schema)
    reader.setPage(page)
    Iterator.continually {
      if (reader.nextRecord()) {
        val visitor = ColumnVisitor(reader, timestampFormatter())
        schema.visitColumns(visitor)
        visitor.getRecord
      } else {
        reader.close()
        Map.empty[String, AnyRef]
      }
    } takeWhile (_ != Map.empty[String, AnyRef])
  }

  override def commit(): TaskReport = Exec.newTaskReport
  override def abort(): Unit        = ()
  override def finish(): Unit       = ()
  override def close(): Unit        = {
    if (!FluentdOutputPlugin.executeTransaction) {
      sender.close()
    }
    if(FluentdOutputPlugin.executeTransaction && FluentdOutputPlugin.taskCountOpt.contains(taskIndex + 1)) {
      sender.close()
    }
  }
}
