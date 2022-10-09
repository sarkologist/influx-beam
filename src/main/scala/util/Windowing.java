package util;

import org.apache.beam.sdk.transforms.windowing.*;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class Windowing {
    public static <T> Window<T> fixedPanes(Duration size) {
        return Window.<T>into(new GlobalWindows())
                .triggering(Repeatedly.forever(
                        AfterProcessingTime
                                .pastFirstElementInPane()
                                .plusDelayOf(size)
                ))
                .withAllowedLateness(Duration.ZERO)
                .withTimestampCombiner(TimestampCombiner.EARLIEST)
                .discardingFiredPanes();
    }

    public static <T> Window<T> perElementPanes() {
        return Window.<T>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .withAllowedLateness(Duration.ZERO)
                .withTimestampCombiner(TimestampCombiner.EARLIEST)
                .discardingFiredPanes();
    }

}
