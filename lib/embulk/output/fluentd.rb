Embulk::JavaPlugin.register_output(
  "fluentd", "org.embulk.output.fluentd.FluentdOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
