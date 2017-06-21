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

}
