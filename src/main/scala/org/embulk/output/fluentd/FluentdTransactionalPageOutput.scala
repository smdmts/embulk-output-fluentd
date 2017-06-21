package org.embulk.output.fluentd

import org.embulk.config.{TaskReport, TaskSource}
import org.embulk.spi._

case class FluentdTransactionalPageOutput(taskSource: TaskSource,
                                          schema: Schema,
                                          taskIndex: Int)
    extends TransactionalPageOutput {

  val task: PluginTask = taskSource.loadTask(classOf[PluginTask])

  override def add(page: Page): Unit = {
    Emitter(() => asIterator(page))
  }

  def asIterator(page: Page): Iterator[Map[String, AnyRef]] = {
    val reader: PageReader = new PageReader(schema)
    reader.setPage(page)
    Iterator.continually {
      if (reader.nextRecord()) {
        val visitor = ColumnVisitor(reader)
        schema.visitColumns(visitor)
        visitor.getRecord
      } else {
        reader.close()
        Map.empty[String, AnyRef]
      }
    } takeWhile (_ != Map.empty[String, AnyRef])
  }

  override def commit(): TaskReport = Exec.newTaskReport
  override def abort(): Unit = ()
  override def finish(): Unit = ()
  override def close(): Unit = ()
}
