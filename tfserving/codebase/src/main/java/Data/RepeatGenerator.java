package Data;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.utils.ThrottledIterator;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

public class RepeatGenerator implements Iterator<float[][][][]>, Serializable {
    private static final Random rand = new Random();
    private static final int IMAGE_SIZE = 224;
    private final int batchSize;
    private final int experimentTime;

    private int warmupRequestsNum;
    private boolean finishedWarmUp;
    private long startTime;
    private static String path;

    RepeatGenerator(String path,int batchSize, int experimentTimeInSeconds, int warmupRequestsNum) {
        this.batchSize = batchSize;
        this.experimentTime = experimentTimeInSeconds * 1000;
        this.warmupRequestsNum = warmupRequestsNum;
        this.path=path;
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
        float[][][][] batchData = new float[batchSize][IMAGE_SIZE][IMAGE_SIZE][3];
        for (int imgNum = 0; imgNum < this.batchSize; imgNum++) {
            BufferedImage bimg = null;
            try {
                bimg = ImageIO.read(new File(path));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            BufferedImage resizedImage = new BufferedImage(224, 224, bimg.getType());
            resizedImage.getGraphics().drawImage(bimg, 0, 0, 224, 224, null);
            for (int i = 0; i < 224; i++) {
                for (int j = 0; j < 224; j++) {
                    int pixel = resizedImage.getRGB(j, i);
                    for (int c = 3 - 1; c >= 0; c--) {
                        batchData[imgNum][i][j][2 - c] = ((pixel >> c * 8) & 0xFF) / 255.0f;
                    }
                }
            }
            // generate 0-filled image
            // add one random value for each image
            // append the new image to the batch

        }
        return batchData;
    }

    public static DataStream<float[][][][]> getThrottledSource(
            StreamExecutionEnvironment env,
            long inputRate, int batchSize,
            int experimentTimeInSeconds,
            int warmupRequestsNum) {
        return env.fromCollection(
                new ThrottledIterator<>(new RepeatGenerator(path,batchSize, experimentTimeInSeconds, warmupRequestsNum),
                        inputRate),
                TypeInformation.of(new TypeHint<float[][][][]>() {}));
    }

    public static DataStream<float[][][][]> getSource(StreamExecutionEnvironment env,
                                                      int batchSize,
                                                      int experimentTimeInSeconds,
                                                      int warmupRequestsNum) {
        return env.fromCollection(new RepeatGenerator(path,batchSize, experimentTimeInSeconds, warmupRequestsNum),
                TypeInformation.of(new TypeHint<float[][][][]>() {}));
    }
}