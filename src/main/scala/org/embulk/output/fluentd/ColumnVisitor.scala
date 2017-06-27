package org.embulk.output.fluentd

import org.embulk.spi.time.TimestampFormatter
import org.embulk.spi.{Column, PageReader, ColumnVisitor => EmbulkColumnVisitor}

case class ColumnVisitor(reader: PageReader, timestampFormatter: TimestampFormatter) extends EmbulkColumnVisitor {
  import scala.collection.mutable

  private val record = mutable.Map[String, AnyRef]()

  override def timestampColumn(column: Column): Unit =
    value(column, reader.getTimestamp).foreach(v => put(column, timestampFormatter.format(v)))

  override def stringColumn(column: Column): Unit =
    value(column, reader.getString).foreach(v => put(column, v))

  override def longColumn(column: Column): Unit =
    value(column, reader.getLong).foreach(v => put(column, Long.box(v)))

  override def doubleColumn(column: Column): Unit =
    value(column, reader.getDouble).foreach(v => put(column, Double.box(v)))

  override def booleanColumn(column: Column): Unit =
    value(column, reader.getBoolean).foreach(v => put(column, Boolean.box(v)))

  override def jsonColumn(column: Column): Unit =
    value(column, reader.getJson).foreach(v => put(column, v.toJson))

  def value[A](column: Column, method: => (Column => A)): Option[A] =
    if (reader.isNull(column)) {
      None
    } else {
      Some(method(column))
    }

  def put[A <: AnyRef](column: Column, value: A): Unit = {
    record.put(column.getName, value)
    ()
  }

  def getRecord: Map[String, AnyRef] = record.toMap

}
