package util

import com.spotify.scio.coders.{Beam, Coder, CoderMaterializer}
import org.apache.beam.sdk.coders.{CoderRegistry, KvCoder, Coder => BeamCoder}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.values.KV

object CoderUtil {
  def beamCoderFor[A: Coder]: BeamCoder[A] =
    toBeam(Coder[A])

  def toBeam[A](coder: Coder[A]): BeamCoder[A] =
    CoderMaterializer.beam(CoderRegistry.createDefault(),
                           PipelineOptionsFactory.create(),
                           coder)

  def beamKvCoderFor[K: Coder, V: Coder]: BeamCoder[KV[K, V]] =
    toBeam(Beam(KvCoder.of(beamCoderFor[K], beamCoderFor[V])))

  // overrides scio's inference of the coder for KV
  // e.g. to avoid: Exception in thread "main" java.lang.IllegalArgumentException: ParDo requires its input to use KvCoder in order to use state and timers.
  implicit def scioInfersWrongKvCoderImplicit[K: Coder, V: Coder]
    : Coder[KV[K, V]] =
    Beam(KvCoder.of(beamCoderFor[K], beamCoderFor[V]))
}
