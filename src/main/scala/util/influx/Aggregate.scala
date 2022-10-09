package util.influx

import com.spotify.scio.coders.Coder
import util.CoderUtil
import io.razem.influxdbclient.Parameter.Precision
import io.razem.influxdbclient._
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn
import org.apache.beam.sdk.transforms.{Combine, PTransform}
import org.apache.beam.sdk.values.{KV, PCollection, TupleTag, TupleTagList}

import scala.jdk.CollectionConverters._
import util.PCollectionPimp._

object Aggregate {
  object Tags {
    final val max = new TupleTag[NonEmptyFactored[AggregatedValue]] {}
    final val min = new TupleTag[NonEmptyFactored[AggregatedValue]] {}
    final val avg = new TupleTag[NonEmptyFactored[AggregatedValue]] {}
    final val sum = new TupleTag[NonEmptyFactored[AggregatedValue]] {}

    final val raw = new TupleTag[InfluxPoint] {}

    val combiners: Map[AggregationType, BinaryCombiner[AggregatedValue]] =
      Map(
        Max -> new BinaryCombiner(Aggregate.combineMax),
        Min -> new BinaryCombiner(Aggregate.combineMin),
        Sum -> new BinaryCombiner(Aggregate.combineSum),
        // TODO: support avg
      )

    val aggregations =
      Map(
        Tags.max -> Aggregation("max",
                                new BinaryCombiner(Aggregate.combineMax),
                                identity[NonEmptyFactored[AggregatedValue]],
                                identity[Factored[AggregatedValue]]),
        Tags.min -> Aggregation("min",
                                new BinaryCombiner(Aggregate.combineMin),
                                identity[NonEmptyFactored[AggregatedValue]],
                                identity[Factored[AggregatedValue]]),
        Tags.sum -> Aggregation("sum",
                                new BinaryCombiner(Aggregate.combineSum),
                                identity[NonEmptyFactored[AggregatedValue]],
                                identity[Factored[AggregatedValue]]),
        Tags.avg -> Aggregation(
          "avg",
          new BinaryCombiner(Aggregate.combineAvg),
          (n: NonEmptyFactored[AggregatedValue]) => n.map(Averaged(1, _)),
          (n: Factored[Averaged]) => n.map[AggregatedValue](_.value)
        )
      )

    val routing: Map[AggregationType, TupleTag[_]] = Map(
      Max           -> Tags.max,
      Min           -> Tags.min,
      Avg           -> Tags.avg,
      Sum           -> Tags.sum,
      NoAggregation -> Tags.raw)

    def metricTags(): TupleTagList =
      TupleTagList.of(
        routing.values.toList.asJava.asInstanceOf[java.util.List[TupleTag[_]]])
  }

  sealed trait AggregationType
  case object Min           extends AggregationType
  case object Max           extends AggregationType
  case object Avg           extends AggregationType
  case object Sum           extends AggregationType
  case object NoAggregation extends AggregationType

  sealed trait AggregatedValue
  case class LongValue(value: Long)     extends AggregatedValue
  case class DoubleValue(value: Double) extends AggregatedValue

  case class Factor(measurement: String,
                    tags: Map[String, String],
                    fieldName: String) {
    def mkDeterministicStringKey: String = {
      val b = new StringBuilder()
      b.append(measurement)
      tags.toSeq.sortBy(_._1).foreach {
        case (k, v) => b.append(Tag(k, v).serialize)
      }
      b.append(fieldName)
      b.toString()
    }
  }

  def makeFactor(measurement: String,
                 tags: Map[String, String],
                 value: AggregatedValue,
                 time: Long): NonEmptyFactored[AggregatedValue] =
    NonEmptyFactored(
      Factor(measurement, tags, "value"),
      time,
      value
    )

  sealed trait Metric
  sealed trait Factored[+T] extends Metric {
    def map[U](f: T => U): Factored[U]
  }
  case object EmptyFactored extends Factored[Nothing] {
    def map[U](f: Nothing => U) = EmptyFactored
  }
  // we cannot have multiple fields because we need to aggregate to a single timestamp in the influx line protocol
  case class NonEmptyFactored[T](factor: Factor, timestamp: Long, value: T)
      extends Factored[T] {
    def mkDeterministicStringKey: String       = factor.mkDeterministicStringKey
    def map[U](f: T => U): NonEmptyFactored[U] = this.copy(value = f(value))
  }

  def aggregatedToField(key: String, v: AggregatedValue): Field = v match {
    case LongValue(x)   => LongField(key, x)
    case DoubleValue(x) => DoubleField(key, x)
  }

  def mkPoint(f: Factored[AggregatedValue]): Option[InfluxPoint] = {
    f match {
      case NonEmptyFactored(k, t, value) =>
        Some(
          InfluxPoint.mkPoint(k.measurement,
                              t,
                              Precision.MILLISECONDS,
                              k.tags.toList.map((Tag.apply _).tupled),
                              List(aggregatedToField(k.fieldName, value))))
      case EmptyFactored => None
    }
  }

  // this is a parallelisable fold, where S is a semigroup
  case class Aggregation[I, S, O](name: String, // for display purposes
                                  binaryCombineFn: BinaryCombineFn[S],
                                  in: I => S,
                                  out: S => O)

  class Aggregator[K: Coder, I, S: Coder, O](aggregation: Aggregation[I, S, O])
      extends PTransform[PCollection[KV[K, I]], PCollection[KV[K, O]]] {
    override def expand(input: PCollection[KV[K, I]]): PCollection[KV[K, O]] =
      input
        .map("aggregation in",
             (kv: KV[K, I]) => KV.of(kv.getKey, aggregation.in(kv.getValue)))
        .setCoder(
          KvCoder.of(CoderUtil.beamCoderFor[K], CoderUtil.beamCoderFor[S]))
        .apply(Combine.perKey(aggregation.binaryCombineFn))
        .map("aggregation out",
             (kv: KV[K, S]) => KV.of(kv.getKey, aggregation.out(kv.getValue)))
  }

  // takes care of two things
  // 1. preserves the factor
  // 2. handles case of aggregating no values
  class BinaryCombiner[T](f: ((Long, T), (Long, T)) => (Long, T))
      extends BinaryCombineFn[Factored[T]] {
    override def apply(left: Factored[T], right: Factored[T]): Factored[T] =
      (left, right) match {
        case (NonEmptyFactored(factor, t1, f1), NonEmptyFactored(_, t2, f2)) =>
          val (t3, f3) = f((t1, f1), (t2, f2))
          NonEmptyFactored(factor, t3, f3)
        case (EmptyFactored, x) => x
        case (x, EmptyFactored) => x
      }

    override def identity(): Factored[T] = EmptyFactored
  }

  case class Averaged(count: Long, value: AggregatedValue)

  def combineAvg(p: (Long, Averaged), q: (Long, Averaged)): (Long, Averaged) =
    (p, q) match {
      case ((s, Averaged(m, DoubleValue(x))),
            (t, Averaged(n, DoubleValue(y)))) =>
        ((m * s + n * t) / (m + n),
         Averaged(m + n, DoubleValue((m * x + n * y) / (m + n))))
      case ((s, Averaged(m, LongValue(x))), (t, Averaged(n, LongValue(y)))) =>
        ((m * s + n * t) / (m + n),
         Averaged(m + n, LongValue((m * x + n * y) / (m + n))))
    }

  def combineSum(p: (Long, AggregatedValue),
                 q: (Long, AggregatedValue)): (Long, AggregatedValue) = {
    val value = (p._2, q._2) match {
      case (x: DoubleValue, y: DoubleValue) => DoubleValue(x.value + y.value)
      case (x: LongValue, y: LongValue)     => LongValue(x.value + y.value)
      case _ =>
        throw new IllegalArgumentException("cannot combine different types")
    }
    (p._1 max q._1, value)
  }

  def combineMax(p: (Long, AggregatedValue),
                 q: (Long, AggregatedValue)): (Long, AggregatedValue) =
    (p._2, q._2) match {
      case (x: DoubleValue, y: DoubleValue) =>
        if (x.value > y.value) p else q
      case (x: LongValue, y: LongValue) =>
        if (x.value > y.value) p else q
      case _ =>
        throw new IllegalArgumentException("cannot combine different types")
    }

  def combineMin(p: (Long, AggregatedValue),
                 q: (Long, AggregatedValue)): (Long, AggregatedValue) =
    (p._2, q._2) match {
      case (x: DoubleValue, y: DoubleValue) =>
        if (x.value < y.value) p else q
      case (x: LongValue, y: LongValue) =>
        if (x.value < y.value) p else q
      case _ =>
        throw new IllegalArgumentException("cannot combine different types")
    }
}
