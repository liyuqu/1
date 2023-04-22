package Data;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.examples.utils.ThrottledIterator;

import java.util.ArrayList;

public class RepeatedGeneratorSource extends RichParallelSourceFunction<float[][][][]> {
    private final RepeatGenerator generator;
    private ThrottledIterator<float[][][][]> throttledIterator;
    private boolean shouldBeThrottled = false;

    public RepeatedGeneratorSource(String path,int batchSize, int experimentTimeInSeconds, int warmupRequestsNum, int inputRate) {
        this.generator = new RepeatGenerator(path,batchSize, experimentTimeInSeconds, warmupRequestsNum);
        // An input rate equal to 0 means that the source should not be throttled
        if (inputRate > 0) {
            this.shouldBeThrottled = true;
            this.throttledIterator = new ThrottledIterator<>(generator, inputRate / 2);
        }
    }

    @Override
    public void run(SourceContext<float[][][][]> sourceContext) throws Exception {
        if (this.shouldBeThrottled) {
            while (this.throttledIterator.hasNext()) {
                sourceContext.collect(this.throttledIterator.next());
            }
        } else {
            while (this.generator.hasNext()) {
                sourceContext.collect(this.generator.next());
            }
        }
    }

    @Override
    public void cancel() {}
}