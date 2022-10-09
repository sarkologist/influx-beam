package util.influx

import util.influx.Aggregate._
import io.razem.influxdbclient.Parameter.Precision
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.TupleTag
import org.joda.time.Instant

/*
Influx metrics involves two parts:
- Beam pipeline changes for metrics aggregation/output steps
- PTransform changes for specifying what metrics to compute

This module deals with the latter.
The former is dealt with by `CollectMetrics` and `MetricsBox`
 */
trait HasMetrics {
  implicit val metrics: MetricsRank2
  val measurementPrefix: String

  var cache: Map[(AggregationType, String), Factored[AggregatedValue]] =
    Map.empty.withDefaultValue(EmptyFactored)

  def flushPreaggregatedMetrics[I, O](c: DoFn[I, O]#WindowedContext): Unit = {
    cache.iterator.foreach {
      case ((aggregation, _), factored) =>
        metrics(c).writeFactor(
          aggregation,
          factored.asInstanceOf[NonEmptyFactored[AggregatedValue]])
    }

    cache = Map.empty.withDefaultValue(EmptyFactored)
  }

  def preCount(measurement: String,
               tags: Map[String, String] = Map.empty,
               number: Long = 1,
               time: Long = Instant.now().getMillis): Unit =
    preAggregate(Aggregate.Sum, measurement, LongValue(number), tags, time)

  /*
  computing aggregates with the beam pipeline graph can put too much stress on the runner
  since it involves grouping operations which entail shuffling which is expensive
  pre-aggregation helps avoid this by maintaining a cache of aggregated values before materialising the output
   */
  def preAggregate(aggregation: AggregationType,
                   measurement: String,
                   number: AggregatedValue,
                   tags: Map[String, String] = Map.empty,
                   time: Long = Instant.now().getMillis): Unit = {
    val factored = Aggregate.makeFactor(measurement, tags, number, time)
    val combiner =
      Tags.combiners(aggregation)

    cache = cache.updated(
      (aggregation, factored.mkDeterministicStringKey),
      combiner(cache((aggregation, factored.mkDeterministicStringKey)),
               factored.asInstanceOf[NonEmptyFactored[AggregatedValue]])
    )
  }

}

trait Metrics extends Serializable {
  def write(measurement: String,
            modify: InfluxPoint => InfluxPoint = identity,
            time: Long = Instant.now().getMillis,
            precision: Precision.Precision = Precision.MILLISECONDS): Unit

  def writeFactor(aggregation: AggregationType,
                  factor: NonEmptyFactored[AggregatedValue]): Unit

  def writeAggregated(measurement: String,
                      tags: Map[String, String],
                      aggregation: AggregationType,
                      value: AggregatedValue,
                      time: Long): Unit =
    writeFactor(aggregation, makeFactor(measurement, tags, value, time))

  def count(measurement: String,
            tags: Map[String, String] = Map.empty,
            number: Long = 1,
            time: Long = Instant.now().getMillis): Unit =
    aggregate(Aggregate.Sum, measurement, LongValue(number), tags, time)

  def aggregate(aggregation: AggregationType,
                measurement: String,
                number: AggregatedValue,
                tags: Map[String, String] = Map.empty,
                time: Long = Instant.now().getMillis): Unit =
    writeAggregated(measurement, tags, aggregation, number, time)

}

trait MetricsRank2 extends Serializable {
  def apply[I, O](c: DoFn[I, O]#WindowedContext): Metrics
}

object Metrics {
  implicit def influxMetrics: MetricsRank2 = new MetricsRank2 {
    override def apply[I, O](c: DoFn[I, O]#WindowedContext): Metrics =
      new Metrics {
        def writeFactor(aggregation: AggregationType,
                        value: NonEmptyFactored[AggregatedValue]): Unit = {
          val tag: TupleTag[_] = Aggregate.Tags.routing(aggregation)
          tag match {
            case t: TupleTag[NonEmptyFactored[AggregatedValue]] =>
              c.output(t, value)
          }
        }

        def write(
            measurement: String,
            modify: InfluxPoint => InfluxPoint,
            time: Long,
            precision: Precision.Precision = Precision.MILLISECONDS): Unit =
          c.output(Tags.raw,
                   modify(InfluxPoint.mkPoint(measurement, time, precision)))
      }
  }

  val noOp: Metrics = new Metrics {
    def writeFactor(
        aggregation: AggregationType,
        value: NonEmptyFactored[AggregatedValue],
    ): Unit = ()

    def write(measurement: String,
              modify: InfluxPoint => InfluxPoint,
              time: Long,
              precision: Precision.Precision = Precision.MILLISECONDS): Unit =
      ()
  }

  implicit val noOpMetrics: MetricsRank2 =
    new MetricsRank2 {
      override def apply[I, O](ignored: DoFn[I, O]#WindowedContext): Metrics = {
        noOp
      }
    }
}
