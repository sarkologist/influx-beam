package util

import java.lang.{Boolean => JBoolean, Iterable => JIterable}

import com.spotify.scio.coders.Coder
import util.CoderUtil.{beamCoderFor, beamKvCoderFor}
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple
import org.apache.beam.sdk.transforms.{
  Filter,
  FlatMapElements,
  GroupByKey,
  InferableFunction,
  MapElements,
  PTransform,
  Reshuffle,
  Wait
}
import org.apache.beam.sdk.values.{KV, PCollection, PInput, POutput}

import scala.jdk.CollectionConverters._

object PCollectionPimp {
  implicit class PCollectionCanFunction[A](pCollection: PCollection[A]) {
    def map[B: Coder](name: String = null, f: A => B): PCollection[B] = {
      val transform =
        MapElements.via(new InferableFunction[A, B]() {
          override def apply(input: A): B = f(input)
        })
      pCollection
        .apply(Option(name).getOrElse(transform.getName), transform)
        .setCoder(beamCoderFor[B])
    }

    def mapKv[K: Coder, V, W: Coder](name: String = null, f: V => W)(
        implicit ev: A =:= KV[K, V]
    ): PCollection[KV[K, W]] = {
      pCollection
        .map(name, kv => KV.of(kv.getKey, f(kv.getValue)))
        .setCoder(KvCoder.of(beamCoderFor[K], beamCoderFor[W]))
    }

    def filter(name: String = null, f: A => Boolean): PCollection[A] = {
      val transform: Filter[A] =
        Filter.by(new InferableFunction[A, JBoolean]() {
          override def apply(input: A): JBoolean = f(input)
        })

      pCollection
        .apply(Option(name).getOrElse(transform.getName), transform)
    }

    def flatMap[B: Coder](name: String = null,
                          f: A => Iterable[B]): PCollection[B] = {
      val transform =
        FlatMapElements.via(new InferableFunction[A, JIterable[B]]() {
          override def apply(input: A): JIterable[B] = f(input).asJava
        })

      pCollection
        .apply(Option(name).getOrElse(transform.getName), transform)
        .setCoder(beamCoderFor[B])
    }
  }

  implicit class PCollectionCanWait[A: Coder](pCollection: PCollection[A]) {
    def waitOn[B](waitedOn: PCollection[B]*): PCollection[A] =
      pCollection
        .applyWithNamePrefix(
          pCollection.getName,
          Wait.on(waitedOn: _*): Wait.OnSignal[A]
        )
        .setCoder(beamCoderFor[A])
  }

  implicit class PCollectionCanName[A](pCollection: PCollection[A]) {
    def applyWithNamePrefix[PA >: PCollection[A] <: PInput, B <: POutput](
        prefix: String,
        transform: PTransform[PA, B]
    ): B =
      pCollection
        .apply(prefix + "/" + transform.getName, transform)
  }

  implicit class KeyedPCollectionTupleCanName[A](
      pCollection: KeyedPCollectionTuple[A]
  ) {
    def applyWithNamePrefix[X, B <: POutput](
        prefix: String,
        transform: PTransform[KeyedPCollectionTuple[A], B]
    ): B =
      pCollection
        .apply(prefix + "/" + transform.getName, transform)
  }
}
