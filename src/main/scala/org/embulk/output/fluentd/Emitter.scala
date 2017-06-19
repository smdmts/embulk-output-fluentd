package org.embulk.output.fluentd

import org.komamitsu.fluency.Fluency
import scala.collection.JavaConverters._

object Emitter {
  private var emitter: Option[Fluency] = None
  private var tag: Option[String] = None
  def init(task: PluginTask): Unit = {
    val config = new Fluency.Config()
      .setBufferChunkInitialSize(task.getBufferChunkInitialSize)
      .setBufferChunkRetentionSize(task.getBufferChunkRetentionSize)
      .setWaitUntilBufferFlushed(task.getWaitUntilBufferFlushed)
      .setWaitUntilFlusherTerminated(task.getWaitUntilFlusherTerminated)
    emitter = Some(
      Fluency.defaultFluency(
        task.getHost,
        task.getPort,
        config
      ))
    tag = Some(task.getTag)
  }

  // TODO retryer.
  def emit(value: Map[String, AnyRef]): Unit = {
    emitter.foreach(_.emit(tag.get, value.asJava))
  }

  def close(): Unit = {
    emitter.foreach { v =>
      v.close()
    }
  }

}
