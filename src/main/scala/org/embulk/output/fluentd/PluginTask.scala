package org.embulk.output.fluentd

import org.embulk.config.{Config, ConfigDefault, Task}

trait PluginTask extends Task {

  @Config("host")
  @ConfigDefault("\"127.0.0.1\"")
  def getHost: String

  @Config("port")
  @ConfigDefault("24224")
  def getPort: Int

  @Config("tag")
  def getTag: String

  @Config("bufferChunkInitialSize")
  @ConfigDefault("410241024") // 4 * 1024 * 1024
  def getBufferChunkInitialSize: Int

  @Config("BufferChunkRetentionSize")
  @ConfigDefault("1610241024") // 16 * 1024 * 1024
  def getBufferChunkRetentionSize: Int

  @Config("WaitUntilBufferFlushed")
  @ConfigDefault("30")
  def getWaitUntilBufferFlushed: Int

  @Config("WaitUntilFlusherTerminated")
  @ConfigDefault("40")
  def getWaitUntilFlusherTerminated: Int

}
