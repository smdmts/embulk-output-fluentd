package org.embulk.output.fluentd

import org.embulk.config.{Config, ConfigDefault, Task}
import org.embulk.spi.time.TimestampFormatter

trait PluginTask extends Task with TimestampFormatter.Task with TimestampFormatter.TimestampColumnOption {

  @Config("host")
  @ConfigDefault("\"127.0.0.1\"")
  def getHost: String

  @Config("port")
  @ConfigDefault("24224")
  def getPort: Int

  @Config("sendingAsyncSize")
  @ConfigDefault("4")
  def getSendingAsyncSize: Int

  @Config("tag")
  def getTag: String

}
