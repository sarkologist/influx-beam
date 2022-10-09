package util.influx

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.{PipelineOptions, Validation, ValueProvider}
import org.joda.time.Duration
import util.ValueProviderPimp._
import scalaz.syntax.applicative._

object InfluxConfig {
  trait InfluxPipelineOptions extends PipelineOptions {
    // PipelineOptions.getJobName is not a ValueProvider, so does not work with templates
    @Validation.Required
    def getWorkaroundJobName: ValueProvider[String]
    def setWorkaroundJobName(value: ValueProvider[String]): Unit

    @Validation.Required
    def getInfluxHost: ValueProvider[String]
    def setInfluxHost(value: ValueProvider[String]): Unit

    @Validation.Required
    def getInfluxPort: ValueProvider[Integer]
    def setInfluxPort(value: ValueProvider[Integer]): Unit

    @Validation.Required
    def getInfluxDatabase: ValueProvider[String]
    def setInfluxDatabase(value: ValueProvider[String]): Unit
  }

  def makeMetricsBox(
      p: Pipeline,
      influxOptions: InfluxPipelineOptions,
      projectId: ValueProvider[String],
      aggregationInterval: Duration = Duration.standardSeconds(1),
  ): MetricsBox.MetricsBox =
    MetricsBox
      .MetricsBox(p,
                  influxOptions.getInfluxHost,
                  influxOptions.getInfluxPort,
                  influxOptions.getInfluxDatabase,
                  aggregationInterval)
      .withMetadata((projectId |@| influxOptions.getWorkaroundJobName) {
        (project, jobName) =>
          _.addTag("project", project)
            .addTag("job_name", jobName)
      })
}
