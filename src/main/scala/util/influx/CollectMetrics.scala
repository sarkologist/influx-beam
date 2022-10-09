package util.influx

import util.influx.Aggregate.Tags
import org.apache.beam.sdk.transforms.{PTransform, ParDo}
import org.apache.beam.sdk.values.{
  PCollection,
  PCollectionTuple,
  TupleTag,
  TupleTagList
}

/*
Influx metrics involves two parts:
- Beam pipeline changes for metrics aggregation/output steps
- PTransform changes for specifying what metrics to compute

Program code specifying the metrics themselves are handled by `Metrics`.
Pipeline metrics aggregation/output is handled by `MetricsBox`

Wrappers for collecting metrics from PTransforms
- this mainly involves setting up output tags for wrapped transforms.
- will do nothing if the MetricsBox is disabled
- unfortunately we cannot defer wiring to `MetricsBox.wire()` since
  wiring of the graph downstream of the wrapped transform works with a fully realised source, i.e.
  before you do `wrapped.apply(transform)`, whether or not wrapped has additional output tags added has to already have been decided

to wrap a PTransform p, do this

val MetricsBox = new MetricsBox(...)
pipeline.apply(CollectMetrics.from(p).into(box))

instead of

pipeline.apply(p)
 */
object CollectMetrics {
  def from[I, O](
      parDo: ParDo.SingleOutput[I, O]): SingleOutputAwaitingMetricsBox[I, O] = {
    new SingleOutputAwaitingMetricsBox(parDo)
  }

  def from[I, O](
      parDo: ParDo.MultiOutput[I, O]): MultiOutputAwaitingMetricsBox[I, O] = {
    new MultiOutputAwaitingMetricsBox[I, O](parDo)
  }

  class SingleOutputAwaitingMetricsBox[I, O](
      original: ParDo.SingleOutput[I, O]) {
    def into(metricsBox: MetricsBox.MetricsBox)
      : PTransform[PCollection[I], PCollection[O]] = {
      if (metricsBox.enabled) {
        new SingleOutputWrapped(
          original.withOutputTags(new TupleTag[O](originalTag),
                                  Tags.metricTags()),
          metricsBox)
      } else original.asInstanceOf[PTransform[PCollection[I], PCollection[O]]]
    }
  }

  class MultiOutputAwaitingMetricsBox[I, O](original: ParDo.MultiOutput[I, O]) {
    def into(metricsBox: MetricsBox.MetricsBox)
      : PTransform[PCollection[I], PCollectionTuple] =
      if (metricsBox.enabled) {
        val sideOutputs: TupleTagList =
          original.getAdditionalOutputTags.and(Tags.metricTags().getAll)
        new MultiOutputWrapped(
          ParDo
            .of(original.getFn)
            .withOutputTags(original.getMainOutputTag, sideOutputs),
          metricsBox)
      } else original.asInstanceOf[PTransform[PCollection[I], PCollectionTuple]]
  }

  class SingleOutputWrapped[I, O](wrapped: ParDo.MultiOutput[I, O],
                                  box: MetricsBox.MetricsBox)
      extends PTransform[PCollection[I], PCollection[O]] {
    override def expand(input: PCollection[I]): PCollection[O] = {
      val result = input
        .apply(wrapped)
      box.add(result)
      // we try to keep the ParDo.SingleOutput as a ParDo.SingleOutput
      result
        .get(new TupleTag[O](originalTag))
    }
  }

  class MultiOutputWrapped[I, O](wrapped: ParDo.MultiOutput[I, O],
                                 box: MetricsBox.MetricsBox)
      extends PTransform[PCollection[I], PCollectionTuple] {
    override def expand(input: PCollection[I]): PCollectionTuple = {
      val result: PCollectionTuple = input
        .apply(wrapped)
      box.add(result)
      // it is already a ParDo.MultiOutput, return the same ParDo.MultiOutput
      result
    }
  }

  final val originalTag = "original"
}
