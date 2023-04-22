package Request;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.net.URL;
import java.util.Collections;


public class AsyncInferenceRequestInt
        extends RichAsyncFunction<int[][][][], Tuple4<String, Long, Long, int[][][][]>> {
    private static final String TENSORFLOW_URL = "http://127.0.0.1:8501/v1/models/0_class:predict";
    private static final String CLASS1_URL = "http://127.0.0.1:8501/v1/models/1_class:predict";
    private final URL url1;
    private int batchSize;

    public AsyncInferenceRequestInt(String url1) throws Exception {
        // Create a neat value object to hold the URL
        this.url1 = new URL(url1);
    }

    @Override
    public void asyncInvoke(int[][][][] inputBatch,
                            ResultFuture<Tuple4<String, Long, Long,int[][][][]>> resultFuture) throws Exception {
        long startTime = System.nanoTime();
        // Append all the edges to a string
        this.batchSize = inputBatch.length;
        String data = InferenceRequest.buildRequest(inputBatch);
        String result = InferenceRequest.makeRequest(data, url1);
        //postprocess the data;
        // NOTE: Measures the duration for the whole batch
        resultFuture.complete(Collections.singleton(new Tuple4<>(result, startTime, System.nanoTime(),inputBatch)));
    }
}
