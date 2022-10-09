package util.influx

import util.CoderUtil
import io.razem.influxdbclient.Parameter.Precision
import io.razem.influxdbclient.{Database, Point}
import org.apache.beam.sdk.options.ValueProvider
import org.apache.beam.sdk.state._
import org.apache.beam.sdk.transforms.DoFn._
import org.apache.beam.sdk.transforms.{DoFn, PTransform, ParDo}
import org.apache.beam.sdk.values.{KV, PCollection, PDone}
import org.joda.time.Duration
import org.slf4j.{Logger, LoggerFactory}

import java.time.Instant
import scala.util.{Failure, Success}

object InfluxSink {
  type DoFnT[T] = DoFn[KV[T, InfluxPoint], PDone]

  // maximum time before flushing buffered points
  // no reason why it is this specific value other than it works
  val maxLatency: Duration = Duration.standardSeconds(60)

  class InfluxSinkTransform[T](host: ValueProvider[String],
                               port: ValueProvider[Integer],
                               database: ValueProvider[String],
                               enrichPoint: ValueProvider[Point => Point],
                               // maximum number of points before buffered points are flushed
                               // no reason why it is this specific value other than it works

                               maxBufferSize: Int)
      extends PTransform[PCollection[KV[T, InfluxPoint]], PDone] {

    override def expand(input: PCollection[KV[T, InfluxPoint]]): PDone = {
      input
        .apply(ParDo.of(new Do[T]))
        .setCoder(CoderUtil.beamCoderFor[PDone])
      PDone.in(input.getPipeline)
    }

    class Do[K] extends DoFnT[K] {
      final val FLUSH_TIMER = "flushTimer"
      @TimerId(FLUSH_TIMER)
      val flushTimerSpec: TimerSpec =
        TimerSpecs.timer(TimeDomain.PROCESSING_TIME)

      final val POINTS = "points"
      @StateId(POINTS)
      val pointsStateDef: StateSpec[ValueState[
        scala.collection.mutable.Map[InfluxPrecision, List[Point]]]] =
        StateSpecs.value(
          CoderUtil.beamCoderFor[
            scala.collection.mutable.Map[InfluxPrecision, List[Point]]])

      var db: Database = _

      @Setup
      def initialise(): Unit = {
        db = InfluxSingleton.getOrInitialise(host.get, port.get, database.get)
      }

      @ProcessElement
      def processElement(
          c: DoFnT[K]#ProcessContext,
          @StateId(POINTS) pointsState: ValueState[
            scala.collection.mutable.Map[InfluxPrecision, List[Point]]],
          @TimerId(FLUSH_TIMER) flushTimer: Timer,
      ): Unit = {
        val points = Option(pointsState.read())
          .getOrElse(scala.collection.mutable.Map.empty)

        if (points.isEmpty) {
          flushTimer.offset(maxLatency).setRelative()
        }
        val point     = c.element.getValue.p
        val precision = c.element.getValue.precision

        points.get(precision) match {
          case Some(xs: List[Point]) =>
            points.update(precision, point :: xs)
          case None => points += (precision -> List(point))
        }
        pointsState.write(points)

        if (points.size > maxBufferSize) { // .size possibly inefficient
          flush(pointsState)
        } else {
          val p = enrichPoint.get()(
            Point("beam_to_influxdb.bundle_points.not_flushed",
                  Instant.now().toEpochMilli)
              .addField("value", points.size)) // .size possibly inefficient

          db.write(p, precision = Precision.MILLISECONDS)
        }
      }

      @OnTimer(FLUSH_TIMER)
      def flush(@StateId(POINTS) pointsState: ValueState[
        scala.collection.mutable.Map[InfluxPrecision, List[Point]]]): Unit = {
        import InfluxSingleton.ec

        Option(pointsState.read()) foreach { m =>
          for ((precision, points) <- m) {
            db.bulkWrite(points.map(enrichPoint.get()), precision.precision)
              .onComplete {
                case Success(true) => ()
                case Failure(e) =>
                  LOG.error("failed writing influx points {}", e)
              }

            points.groupBy(_.key).keys.foreach { measurement =>
              // fire and forget
              db.write(
                enrichPoint.get()(Point("beam.bundle_points")
                  .addField("value", points.size) // .size possibly inefficient
                  .addTag("measurement", measurement)),
                precision = Precision.MILLISECONDS
              )
            }
          }
          pointsState.clear()
        }
      }
    }
  }

  val LOG: Logger = LoggerFactory.getLogger(classOf[InfluxSinkTransform[_]])
}
