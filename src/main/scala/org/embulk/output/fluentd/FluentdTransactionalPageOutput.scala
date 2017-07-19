package org.embulk.output.fluentd

import com.google.common.base.Optional
import org.embulk.config.{TaskReport, TaskSource}
import org.embulk.output.fluentd.sender.Sender
import org.embulk.spi._
import org.embulk.spi.time.TimestampFormatter

case class FluentdTransactionalPageOutput(taskSource: TaskSource,
                                          schema: Schema,
                                          taskIndex: Int,
                                          taskCountOpt: Option[Int],
                                          sender: Sender)
    extends TransactionalPageOutput {

  val task: PluginTask = taskSource.loadTask(classOf[PluginTask])
  val logger           = Exec.getLogger(classOf[FluentdTransactionalPageOutput])

  def timestampFormatter(): TimestampFormatter =
    new TimestampFormatter(task, Optional.absent())

  override def add(page: Page): Unit = {
    sender(asIterator(page).toSeq)
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
  override def finish(): Unit = {
    logger.info(s"finished at " + this)
    // for map/reduce executor.
    if (taskCountOpt.isEmpty) {
      // close immediately.
      sender.close()
    }
  }
  override def close(): Unit = {
    if (taskCountOpt.contains(taskIndex + 1)) {
      sender.close()
    }
  }
}
