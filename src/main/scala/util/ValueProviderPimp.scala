package util

import org.apache.beam.sdk.options.ValueProvider
import org.apache.beam.sdk.options.ValueProvider.{
  NestedValueProvider,
  StaticValueProvider
}
import scalaz.{Applicative, Functor}

object ValueProviderPimp {

  implicit class AValueProvider[A](v: ValueProvider[A])
      extends ValueProvider[A] {
    override def isAccessible: Boolean = v.isAccessible
    override def get(): A              = v.get()
    def underlying: ValueProvider[A]   = v

    def ap[B](f: AValueProvider[A => B]): AValueProvider[B] =
      new AValueProvider[B](NestedValueProvider.of(v, a => f.get().apply(a))) {
        override def isAccessible: Boolean = v.isAccessible && f.isAccessible
      }
  }

  implicit val ValueProviderInstances
    : Functor[ValueProvider] with Applicative[ValueProvider] =
    new Functor[ValueProvider] with Applicative[ValueProvider] {
      override def map[A, B](fa: ValueProvider[A])(
          f: A => B): ValueProvider[B] =
        new AValueProvider[B](NestedValueProvider.of(fa.underlying, f.apply))

      override def point[A](a: => A): ValueProvider[A] =
        new AValueProvider(StaticValueProvider.of(a))

      override def ap[A, B](fa: => ValueProvider[A])(
          ff: => ValueProvider[A => B]): ValueProvider[B] =
        fa.ap(ff)
    }
}
