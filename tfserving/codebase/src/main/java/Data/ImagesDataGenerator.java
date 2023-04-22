package Data;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.utils.ThrottledIterator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

public class ImagesDataGenerator implements Iterator<float[][][][]>, Serializable {
    private static final Random rand = new Random();
    private static final int IMAGE_SIZE = 224;
    private final int batchSize;
    private final int experimentTime;

    private int warmupRequestsNum;
    private boolean finishedWarmUp;
    private long startTime;

    ImagesDataGenerator(int batchSize, int experimentTimeInSeconds, int warmupRequestsNum) {
        this.batchSize = batchSize;
        this.experimentTime = experimentTimeInSeconds * 1000;
        this.warmupRequestsNum = warmupRequestsNum;

        this.finishedWarmUp = false;
    }

    @Override
    public boolean hasNext() {
        if (!finishedWarmUp) {
            warmupRequestsNum--;
            if (warmupRequestsNum < 0) {
                finishedWarmUp = true;
                this.startTime = System.currentTimeMillis();
            }
        } else {
            if (System.currentTimeMillis() - startTime > experimentTime)
                return false;
        }
        return true;
    }

    @Override
    public float[][][][] next() {
        float[][][][] batch = new float[batchSize][IMAGE_SIZE][IMAGE_SIZE][3];
        for (int imgNum = 0; imgNum < this.batchSize; imgNum++) {
            int randomValIndex1 = rand.nextInt(IMAGE_SIZE);
            int randomValIndex2 = rand.nextInt(IMAGE_SIZE);
            int randomValIndex3 = rand.nextInt(3);
            // generate 0-filled image
            for (int i = 0; i < IMAGE_SIZE; i++) {
                for (int j=0; j<IMAGE_SIZE;j++){
                    for (int h=0;h<3;h++){
                        batch[imgNum][i][j][h]=0.0f;
                    }
                }
            }
            // add one random value for each image
            batch[imgNum][randomValIndex1][randomValIndex2][randomValIndex3]=rand.nextFloat();
            // append the new image to the batch
        }
        return batch;
    }

    public static DataStream<float[][][][]> getThrottledSource(
            StreamExecutionEnvironment env,
            long inputRate, int batchSize,
            int experimentTimeInSeconds,
            int warmupRequestsNum) {
        return env.fromCollection(
                new ThrottledIterator<>(new ImagesDataGenerator(batchSize, experimentTimeInSeconds, warmupRequestsNum),
                        inputRate),
                TypeInformation.of(new TypeHint<float[][][][]>() {}));
    }

    public static DataStream<float[][][][]> getSource(StreamExecutionEnvironment env,
                                                      int batchSize,
                                                      int experimentTimeInSeconds,
                                                      int warmupRequestsNum) {
        return env.fromCollection(new ImagesDataGenerator(batchSize, experimentTimeInSeconds, warmupRequestsNum),
                TypeInformation.of(new TypeHint<float[][][][]>() {}));
    }
}