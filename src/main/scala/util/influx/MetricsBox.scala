package util.influx

import util.influx.Aggregate._
import util.influx.InfluxSink.InfluxSinkTransform
import util.Windowing
import util.CoderUtil
import io.razem.influxdbclient.Point
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.options.ValueProvider
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.values.{KV, PCollectionList, PCollectionTuple}
import org.joda.time.Duration
import scalaz.syntax.applicative._
import util.ValueProviderPimp._

object MetricsBox {
  class ToPoint
      extends DoFn[KV[String, Factored[AggregatedValue]],
                   KV[String, InfluxPoint]] {
    @ProcessElement
    def processElement(c: ProcessContext): Unit = {
      Aggregate
        .mkPoint(c.element.getValue)
        .foreach((p: InfluxPoint) => c.output(KV.of(c.element.getKey, p)))
    }
  }

  /*
  Influx metrics involves two parts:
  - Beam pipeline changes for metrics aggregation/output steps
  - PTransform changes for specifying what metrics to compute

  Program code specifying the metrics themselves are handled by `Metrics`.
  Output tags are handled by `CollectMetrics`

  `MetricsBox` is involved in wiring the Beam pipeline graph for the purposes of processing metrics
  which involves:
  - aggregating metrics
  - outputting metrics to influxdb

  No alteration to the pipeline graph will be performed until `.wire()` is called.
   */
  case class MetricsBox(pipeline: Pipeline,
                        influxHost: ValueProvider[String],
                        influxPort: ValueProvider[Integer],
                        influxDatabase: ValueProvider[String],
                        aggregationInterval: Duration =
                          Duration.standardSeconds(1),
                        enrichPoint: ValueProvider[Point => Point] =
                          StaticValueProvider.of(identity),
                        enabled: Boolean = true) {
    var pointCollection: PCollectionList[KV[String, InfluxPoint]] =
      PCollectionList.empty(pipeline)

    var measuredCollections: Seq[PCollectionTuple] = Seq.empty

    def add(p: PCollectionTuple): Unit = {
      measuredCollections +:= p
    }

    def wireAggregations(p: PCollectionTuple): Unit = {
      val name = "metric aggregation/"
      Tags.aggregations foreach {
        case (tag, aggregation) =>
          val branch = p
            .get(tag)
            .setCoder(CoderUtil.beamCoderFor[NonEmptyFactored[AggregatedValue]])
            .apply(name,
                   WithKeys.of((f: NonEmptyFactored[AggregatedValue]) =>
                     f.mkDeterministicStringKey))
            .setCoder(KvCoder.of(
              CoderUtil.beamCoderFor[String],
              CoderUtil.beamCoderFor[NonEmptyFactored[AggregatedValue]]))
            .apply(name + "window metric aggregation",
                   Windowing.fixedPanes(aggregationInterval))
            .apply(name + "aggregate with " + aggregation.name,
                   new Aggregator(aggregation))
            .setCoder(
              KvCoder.of(CoderUtil.beamCoderFor[String],
                         CoderUtil.beamCoderFor[Factored[AggregatedValue]]))
            .apply(name, ParDo.of(new ToPoint))
            .apply(name + "window metric aggregation",
                   Windowing.fixedPanes[KV[String, InfluxPoint]](
                     aggregationInterval))

          pointCollection = pointCollection.and(
            branch
          )
      }

      pointCollection = pointCollection.and(
        p.get(Tags.raw)
          .apply(name, WithKeys.of((influxP: InfluxPoint) => influxP.p.key))
          .setCoder(KvCoder.of(CoderUtil.beamCoderFor[String],
                               CoderUtil.beamCoderFor[InfluxPoint]))
          .apply(
            name + "window metric aggregation",
            Windowing.fixedPanes[KV[String, InfluxPoint]](aggregationInterval))
      )
    }

    def withMetadata(addMetadata: ValueProvider[Point => Point]): MetricsBox =
      this.copy(enrichPoint = (this.enrichPoint |@| addMetadata) { (old, neu) =>
        old andThen neu
      })

    def disabled: MetricsBox = this.copy(enabled = false)

    def wire(bufferSize: Int = 50000): Unit = if (enabled) {
      measuredCollections.foreach(wireAggregations)

      pointCollection
        .apply(Flatten.pCollections())
        .apply("Sink influx points",
               new InfluxSinkTransform(influxHost,
                                       influxPort,
                                       influxDatabase,
                                       enrichPoint,
                                       bufferSize))
    }
  }

}
