import Data.ImagesDataGeneratorSource;
import Data.RepeatedGeneratorSource;
import Request.AsyncInferenceRequestInt;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import Request.AsyncInferenceRequest;
import Request.InferenceRequest;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import scala.Int;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public class FeedForwardPipeline {
    private static boolean Syn;
    private static int shape=4;
    private static final String TENSORFLOW_URL = "http://127.0.0.1:8501/v1/models/0_class:predict";
    private static final String CLASS1_URL="http://127.0.0.1:8501/v1/models/1_class:predict";
    private static final String OD_URL="http://127.0.0.1:8501/v1/models/od_class:predict";
    private static final String PL_URL="http://127.0.0.1:8501/v1/models/pl_class:predict";
    private static final int ASYNC_OPERATOR_CAPACITY = 10000;
    private static final long ASYNC_OPERATOR_TIMEOUT = 10000;
    private static String path="./testimage/carl.jpg";

    public static void run(int inputRate, int batchSize, int experimentTimeInSeconds, int warmUpRequestsNum,
                           boolean isAsync, int maxInputRatePerThread, String outputFile,int shaped) throws Exception {
        // set up the streaming execution environment
        shape=shaped;
        Syn=!isAsync;
        System.out.println("Run Pipeline"+shape);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Add data sources
        int inputPotentialProducers = (int) Math.ceil((double) inputRate / maxInputRatePerThread);
        int inputProducers = inputPotentialProducers > 0 ? inputPotentialProducers : 1;
        int inputRatePerProducer = Math.min(inputRate, inputRate / inputProducers);
        System.out.println("Input producers: " + inputProducers);
        System.out.println("Input rate per producer: " + inputRatePerProducer);
        env.setParallelism(inputProducers);

        DataStream<float[][][][]> batches = env
            .addSource(
                new ImagesDataGeneratorSource(batchSize, experimentTimeInSeconds, warmUpRequestsNum,
                    inputRatePerProducer));

        DataStream<Tuple4<String, Long, Long,float[][][][]>> d1;
        // send inference request
        if (!Syn) {
            d1 = performRequestAsync(TENSORFLOW_URL, batches);
        }else{
            d1 = performRequest(TENSORFLOW_URL, batches);
        }
        if(shape==1) {
            DataStream<float[][][][]> data1 = d1.map(new MapFunction<Tuple4<String, Long, Long, float[][][][]>, float[][][][]>() {
                @Override
                public float[][][][] map(Tuple4<String, Long, Long, float[][][][]> merge1) throws Exception {
                    String str = merge1.f0.replaceAll("\\s+", "");
                    str = str.replace("{", "").replace("}", "");
                    String[] strArr = str.split(",");
                    float[][] arr2D = new float[batchSize][1001];
                    for (int a = 0; a < strArr.length; a++) {
                        int row = a / 1001;
                        int col = a % 1001;
                        arr2D[row][col] = Float.parseFloat(strArr[a].trim());
                    }
                    float[][][][] newdata = new float[batchSize][224][224][3];
                    for (int b = 0; b < batchSize; b++) {
                        int pos = 0;
                        float max = arr2D[b][0];
                        for (int a = 0; a < arr2D[0].length; a++) {
                            if (arr2D[b][a] > max) {
                                max = arr2D[b][a];
                                pos = a;
                            }
                        }
                        //if (pos == 657 || pos == 480 || pos == 556 || pos == 610 || pos == 628 || pos == 655 || pos == 676 || pos == 706 ||
                        //    pos == 735 || pos == 758 || pos == 780 || pos == 865 || pos == 868) {
                        newdata[b] = merge1.f3[b];
                        //}
                    }
                    return newdata;
                }
            });
            DataStream<Tuple4<String, Long, Long,float[][][][]>> result1;
            if(!Syn){
                result1=performRequestAsync(CLASS1_URL, data1);
            } else{
                result1=performRequest(CLASS1_URL,data1);
            }
            DataStream<Tuple3<String,Long,Long>> result=result1.map(new MapFunction<Tuple4<String, Long, Long, float[][][][]>, Tuple3<String, Long, Long>>() {
                @Override
                public Tuple3<String, Long, Long> map(Tuple4<String, Long, Long, float[][][][]> value) throws Exception {
                    return new Tuple3<>(value.f0, value.f1, value.f2);
                }
            });

        }else if(shape==2){
            DataStream<int[][][][]> data1=d1.map(new MapFunction<Tuple4<String, Long, Long, float[][][][]>, int[][][][]>() {
                @Override
                public int[][][][] map(Tuple4<String, Long, Long, float[][][][]> merge1) throws Exception {
                    String str = merge1.f0.replaceAll("\\s+", "");
                    str = str.replace("{", "").replace("}", "");
                    String[] strArr = str.split(",");
                    float[][] arr2D = new float[batchSize][1001];
                    for (int a = 0; a < strArr.length; a++) {
                        int row = a / 1001;
                        int col = a % 1001;
                        arr2D[row][col] = Float.parseFloat(strArr[a].trim());
                    }
                    int[][][][] newdata = new int[batchSize][224][224][3];
                    for (int b = 0; b < batchSize; b++) {
                        int pos = 0;
                        float max = arr2D[b][0];
                        for (int a = 0; a < arr2D[0].length; a++) {
                            if (arr2D[b][a] > max) {
                                max = arr2D[b][a];
                                pos = a;
                            }
                        }
                        //if (pos == 657 || pos == 480 || pos == 556 || pos == 610 || pos == 628 || pos == 655 || pos == 676 || pos == 706 ||
                          //  pos == 735 || pos == 758 || pos == 780 || pos == 865 || pos == 868) {
                            for (int i = 0; i < 224; i++) {
                                for (int j = 0; j < 224; j++) {
                                    for (int z = 0; z < 3; z++) {
                                        newdata[b][i][j][z] = (int) (merge1.f3[b][i][j][z] * 255);
                                    }
                                }
                            }
                        //}

                    }
                    return newdata;
                }
            });//here we have the data for od model, then we will send it.
            DataStream<Tuple4<String, Long, Long,int[][][][]>> d2;
            if(!Syn){
                d2=performRequestAsyncInt(OD_URL, data1); //result from odmodel
            } else{
                d2=performRequestInt(OD_URL, data1); //result from odmodel
            }
            //so then we need to crop the corresponding image, and resend it to the 1_class node
            //after merge, we need to get the new data send to next node
            DataStream<float[][][][]>data2=d2.map(new MapFunction<Tuple4<String, Long, Long, int[][][][]>, float[][][][]>() {
                @Override
                public float[][][][] map(Tuple4<String, Long, Long, int[][][][]> merge2) throws Exception {
                    float[][][][] newdata = new float[batchSize][224][224][3];
                    for (int a = 0; a < batchSize; a++) {
                        BufferedImage image = new BufferedImage(224, 224, BufferedImage.TYPE_INT_RGB);
                        for (int i = 0; i < 224; i++) {
                            for (int j = 0; j < 224; j++) {
                                int r = (int) (merge2.f3[a][i][j][0] );
                                int g = (int) (merge2.f3[a][i][j][1] );
                                int b = (int) (merge2.f3[a][i][j][2] );
                                int rgb = (r << 16) | (g << 8) | b;
                                image.setRGB(j, i, rgb);
                            }
                        }
                        String str = merge2.f0.replaceAll("\\s+", "");
                        str = str.replace("{", "").replace("}", "").replace(":",",");
                        String[] strArr = str.split(",");
                        //*****Since the result may sometimes get something error because the pretrained model put
                        //every output in one key, like boxes, scores, and numbers (Google DID it!), so we use the
                        //coordinate from my experience
                        //double y1=Float.parseFloat(strArr[1]);
                        //double x1=Float.parseFloat(strArr[2]);
                        //double y2=Float.parseFloat(strArr[3]);
                        //double x2=Float.parseFloat(strArr[4]);
                        //BufferedImage originalImage = image;
                        double y1=0.0;
                        double x1=2.80289459;
                        double y2=223.532211;
                        double x2=223.790222;
                        double width = x2 - x1;
                        double height = y2 - y1;
                        BufferedImage croppedImage = image.getSubimage((int)Math.floor(x1), (int)Math.floor(y1), (int)Math.floor(width), (int)Math.floor(height));
                        int newWidth = 224;
                        int newHeight = 224;
                        Image resizedImage = croppedImage.getScaledInstance(newWidth, newHeight, Image.SCALE_DEFAULT);
                        // Convert the resized image to a BufferedImage object
                        BufferedImage finalImage = new BufferedImage(newWidth, newHeight, BufferedImage.TYPE_INT_RGB);
                        Graphics2D g2d = finalImage.createGraphics();
                        g2d.drawImage(resizedImage, 0, 0, null);
                        g2d.dispose();
                        for (int i = 0; i < 224; i++) {
                            for (int j = 0; j < 224; j++) {
                                int pixel = finalImage.getRGB(j, i);
                                for (int c = 3 - 1; c >= 0; c--) {
                                    newdata[a][i][j][2 - c] = ((pixel >> c * 8) & 0xFF) / 255.0f;
                                }
                            }
                        }
                        //now we get the cropped image, so convert it to the float
                    }
                    return newdata;
                }
            });
            //so we get the new image float can be sent to class1
            DataStream<Tuple4<String,Long,Long,float[][][][]>> d3;
            if(!Syn){
                d3=performRequestAsync(CLASS1_URL,data2);
            }else{
                d3=performRequest(CLASS1_URL,data2);
            }
            //repeat the same step
            DataStream<float[][][][]> data3 = d3.map(new MapFunction<Tuple4<String, Long, Long, float[][][][]>, float[][][][]>() {
                @Override
                public float[][][][] map(Tuple4<String, Long, Long, float[][][][]> merge1) throws Exception {
                    String str = merge1.f0.replaceAll("\\s+", "");
                    str = str.replace("{", "").replace("}", "");
                    String[] strArr = str.split(",");
                    float[][] arr2D = new float[batchSize][1001];
                    for (int a = 0; a < strArr.length; a++) {
                        int row = a / 1001;
                        int col = a % 1001;
                        arr2D[row][col] = Float.parseFloat(strArr[a].trim());
                    }
                    float[][][][] newdata = new float[batchSize][224][224][3];
                    for (int b = 0; b < batchSize; b++) {
                        int pos = 0;
                        float max = arr2D[b][0];
                        for (int a = 0; a < arr2D[0].length; a++) {
                            if (arr2D[b][a] > max) {
                                max = arr2D[b][a];
                                pos = a;
                            }
                        }
                        //if (pos == 657 || pos == 480 || pos == 556 || pos == 610 || pos == 628 || pos == 655 || pos == 676 || pos == 706 ||
                          //  pos == 735 || pos == 758 || pos == 780 || pos == 865 || pos == 868) {
                            newdata[b] = merge1.f3[b];
                        //}
                    }
                    return newdata;
                }
            });
            //get the data, send to last node

            DataStream<Tuple4<String,Long,Long,float[][][][]>>result1;
            if(!Syn){
                result1=performRequestAsync(PL_URL,data3);
            }else{
                result1=performRequest(PL_URL,data3);
            }
            //end for this pipeline
            //0_model->odmodel->1_model->lp_model

            DataStream<Tuple3<String,Long,Long>> result=result1.map(new MapFunction<Tuple4<String, Long, Long, float[][][][]>, Tuple3<String, Long, Long>>() {
                @Override
                public Tuple3<String, Long, Long> map(Tuple4<String, Long, Long, float[][][][]> value) throws Exception {
                    return new Tuple3<>(value.f0, value.f1, value.f2);
                }
            });
        }else if(shape==3){
            //->1_model
            //0_model ->od_model
            //->lp_model
            DataStream<float[][][][]> data1 = d1.map(new MapFunction<Tuple4<String, Long, Long, float[][][][]>, float[][][][]>() {
                @Override
                public float[][][][] map(Tuple4<String, Long, Long, float[][][][]> merge1) throws Exception {
                    String str = merge1.f0.replaceAll("\\s+", "");
                    str = str.replace("{", "").replace("}", "");
                    String[] strArr = str.split(",");
                    float[][] arr2D = new float[batchSize][1001];
                    for (int a = 0; a < strArr.length; a++) {
                        int row = a / 1001;
                        int col = a % 1001;
                        arr2D[row][col] = Float.parseFloat(strArr[a].trim());
                    }
                    float[][][][] newdata = new float[batchSize][224][224][3];
                    for (int b = 0; b < batchSize; b++) {
                        int pos = 0;
                        float max = arr2D[b][0];
                        for (int a = 0; a < arr2D[0].length; a++) {
                            if (arr2D[b][a] > max) {
                                max = arr2D[b][a];
                                pos = a;
                            }
                        }
                       // if (pos == 657 || pos == 480 || pos == 556 || pos == 610 || pos == 628 || pos == 655 || pos == 676 || pos == 706 ||
                         //   pos == 735 || pos == 758 || pos == 780 || pos == 865 || pos == 868) {
                            newdata[b] = merge1.f3[b];
                        //}
                    }
                    return newdata;
                }
            });
            DataStream<int[][][][]> data2=d1.map(new MapFunction<Tuple4<String, Long, Long, float[][][][]>, int[][][][]>() {
                @Override
                public int[][][][] map(Tuple4<String, Long, Long, float[][][][]> merge1) throws Exception {
                    String str = merge1.f0.replaceAll("\\s+", "");
                    str = str.replace("{", "").replace("}", "");
                    String[] strArr = str.split(",");
                    float[][] arr2D = new float[batchSize][1001];
                    for (int a = 0; a < strArr.length; a++) {
                        int row = a / 1001;
                        int col = a % 1001;
                        arr2D[row][col] = Float.parseFloat(strArr[a].trim());
                    }
                    int[][][][] newdata = new int[batchSize][224][224][3];
                    for (int b = 0; b < batchSize; b++) {
                        int pos = 0;
                        float max = arr2D[b][0];
                        for (int a = 0; a < arr2D[0].length; a++) {
                            if (arr2D[b][a] > max) {
                                max = arr2D[b][a];
                                pos = a;
                            }
                        }
                        //if (pos == 657 || pos == 480 || pos == 556 || pos == 610 || pos == 628 || pos == 655 || pos == 676 || pos == 706 ||
                          //  pos == 735 || pos == 758 || pos == 780 || pos == 865 || pos == 868) {
                            for (int i = 0; i < 224; i++) {
                                for (int j = 0; j < 224; j++) {
                                    for (int z = 0; z < 3; z++) {
                                        newdata[b][i][j][z] = (int) (merge1.f3[b][i][j][z] * 255);
                           //         }
                                }
                            }
                        }

                    }
                    return newdata;
                }
            });//here we have the data for od model, then we will send it.


            DataStream<Tuple4<String, Long, Long,float[][][][]>> s1;
            DataStream<Tuple4<String, Long, Long,int[][][][]>> s2;
            DataStream<Tuple4<String, Long, Long,float[][][][]>> s3;
            if (!Syn) {
                s1=performRequestAsync(CLASS1_URL, data1);
                s2=performRequestAsyncInt(OD_URL, data2);
                s3=performRequestAsync(PL_URL, data1);
            }else{
                s1=performRequest(CLASS1_URL, data1);
                s2=performRequestInt(OD_URL, data2);
                s3=performRequest(PL_URL, data1);
            }
            DataStream<Tuple3<String, Long, Long>> c1=s1.map(new MapFunction<Tuple4<String, Long, Long, float[][][][]>, Tuple3<String, Long, Long>>() {
                @Override
                public Tuple3<String, Long, Long> map(Tuple4<String, Long, Long, float[][][][]> value) throws Exception {
                    return new Tuple3<>(value.f0, value.f1, value.f2);
                }
            });
            DataStream<Tuple3<String, Long, Long>> c2=s2.map(new MapFunction<Tuple4<String, Long, Long, int[][][][]>, Tuple3<String, Long, Long>>() {
                @Override
                public Tuple3<String, Long, Long> map(Tuple4<String, Long, Long, int[][][][]> value) throws Exception {
                    return new Tuple3<>(value.f0, value.f1, value.f2);
                }
            });
            DataStream<Tuple3<String, Long, Long>> c3=s3.map(new MapFunction<Tuple4<String, Long, Long, float[][][][]>, Tuple3<String, Long, Long>>() {
                @Override
                public Tuple3<String, Long, Long> map(Tuple4<String, Long, Long, float[][][][]> value) throws Exception {
                    return new Tuple3<>(value.f0, value.f1, value.f2);
                }
            });
            DataStream<Tuple3<String, Long, Long>> result=c1.union(c2).union(c3);
        }else{
            //->od_model
            //0_model            -> lp_model
            //->1_model
            DataStream<float[][][][]> data1 = d1.map(new MapFunction<Tuple4<String, Long, Long, float[][][][]>, float[][][][]>() {
                @Override
                public float[][][][] map(Tuple4<String, Long, Long, float[][][][]> merge1) throws Exception {
                    String str = merge1.f0.replaceAll("\\s+", "");
                    str = str.replace("{", "").replace("}", "");
                    String[] strArr = str.split(",");
                    float[][] arr2D = new float[batchSize][1001];
                    for (int a = 0; a < strArr.length; a++) {
                        int row = a / 1001;
                        int col = a % 1001;
                        arr2D[row][col] = Float.parseFloat(strArr[a].trim());
                    }
                    float[][][][] newdata = new float[batchSize][224][224][3];
                    for (int b = 0; b < batchSize; b++) {
                        int pos = 0;
                        float max = arr2D[b][0];
                        for (int a = 0; a < arr2D[0].length; a++) {
                            if (arr2D[b][a] > max) {
                                max = arr2D[b][a];
                                pos = a;
                            }
                        }
                     //   if (pos == 657 || pos == 480 || pos == 556 || pos == 610 || pos == 628 || pos == 655 || pos == 676 || pos == 706 ||
                       //     pos == 735 || pos == 758 || pos == 780 || pos == 865 || pos == 868) {
                            newdata[b] = merge1.f3[b];
                        //}
                    }
                    return newdata;
                }
            });
            DataStream<int[][][][]> data2=d1.map(new MapFunction<Tuple4<String, Long, Long, float[][][][]>, int[][][][]>() {
                @Override
                public int[][][][] map(Tuple4<String, Long, Long, float[][][][]> merge1) throws Exception {
                    String str = merge1.f0.replaceAll("\\s+", "");
                    str = str.replace("{", "").replace("}", "");
                    String[] strArr = str.split(",");
                    float[][] arr2D = new float[batchSize][1001];
                    for (int a = 0; a < strArr.length; a++) {
                        int row = a / 1001;
                        int col = a % 1001;
                        arr2D[row][col] = Float.parseFloat(strArr[a].trim());
                    }
                    int[][][][] newdata = new int[batchSize][224][224][3];
                    for (int b = 0; b < batchSize; b++) {
                        int pos = 0;
                        float max = arr2D[b][0];
                        for (int a = 0; a < arr2D[0].length; a++) {
                            if (arr2D[b][a] > max) {
                                max = arr2D[b][a];
                                pos = a;
                            }
                        }
                      //  if (pos == 657 || pos == 480 || pos == 556 || pos == 610 || pos == 628 || pos == 655 || pos == 676 || pos == 706 ||
                        //    pos == 735 || pos == 758 || pos == 780 || pos == 865 || pos == 868) {
                            for (int i = 0; i < 224; i++) {
                                for (int j = 0; j < 224; j++) {
                                    for (int z = 0; z < 3; z++) {
                                        newdata[b][i][j][z] = (int) (merge1.f3[b][i][j][z] * 255);
                          //          }
                                }
                            }
                        }

                    }
                    return newdata;
                }
            });//here we have the data for od model, then we will send it.
            DataStream<Tuple4<String,Long,Long,float[][][][]>>s1;
            DataStream<Tuple4<String,Long,Long,int[][][][]>>s2;
            if (!Syn){
                s1=performRequestAsync(CLASS1_URL, data1);
                s2=performRequestAsyncInt(OD_URL, data2);
            }else{
                s1=performRequest(CLASS1_URL, data1);
                s2=performRequestInt(OD_URL, data2);
            }
            //now we get two stream, separately send to lp_model
            //after merge, we need to get the new data send to next node
            DataStream<float[][][][]>data3=s2.map(new MapFunction<Tuple4<String, Long, Long, int[][][][]>, float[][][][]>() {
                @Override
                public float[][][][] map(Tuple4<String, Long, Long, int[][][][]> merge2) throws Exception {
                    float[][][][] newdata = new float[batchSize][224][224][3];
                    for (int a = 0; a < batchSize; a++) {
                        BufferedImage image = new BufferedImage(224, 224, BufferedImage.TYPE_INT_RGB);
                        for (int i = 0; i < 224; i++) {
                            for (int j = 0; j < 224; j++) {
                                int r = (int) (merge2.f3[a][i][j][0] );
                                int g = (int) (merge2.f3[a][i][j][1] );
                                int b = (int) (merge2.f3[a][i][j][2] );
                                int rgb = (r << 16) | (g << 8) | b;
                                image.setRGB(j, i, rgb);
                            }
                        }
                        String str = merge2.f0.replaceAll("\\s+", "");
                        str = str.replace("{", "").replace("}", "").replace(":",",");
                        String[] strArr = str.split(",");
                        //*****Since the result may sometimes get something error because the pretrained model put
                        //every output in one key, like boxes, scores, and numbers (Google DID it!), so we use the
                        //coordinate from my experience
                        //double y1=Float.parseFloat(strArr[1]);
                        //double x1=Float.parseFloat(strArr[2]);
                        //double y2=Float.parseFloat(strArr[3]);
                        //double x2=Float.parseFloat(strArr[4]);
                        //BufferedImage originalImage = image;
                        double y1=0.0;
                        double x1=2.80289459;
                        double y2=223.532211;
                        double x2=223.790222;
                        double width = x2 - x1;
                        double height = y2 - y1;
                        BufferedImage croppedImage = image.getSubimage((int)Math.floor(x1), (int)Math.floor(y1), (int)Math.floor(width), (int)Math.floor(height));
                        int newWidth = 224;
                        int newHeight = 224;
                        Image resizedImage = croppedImage.getScaledInstance(newWidth, newHeight, Image.SCALE_DEFAULT);
                        // Convert the resized image to a BufferedImage object
                        BufferedImage finalImage = new BufferedImage(newWidth, newHeight, BufferedImage.TYPE_INT_RGB);
                        Graphics2D g2d = finalImage.createGraphics();
                        g2d.drawImage(resizedImage, 0, 0, null);
                        g2d.dispose();
                        for (int i = 0; i < 224; i++) {
                            for (int j = 0; j < 224; j++) {
                                int pixel = finalImage.getRGB(j, i);
                                for (int c = 3 - 1; c >= 0; c--) {
                                    newdata[a][i][j][2 - c] = ((pixel >> c * 8) & 0xFF) / 255.0f;
                                }
                            }
                        }
                        //now we get the cropped image, so convert it to the float
                    }
                    return newdata;
                }
            });
            DataStream<float[][][][]> data4 = s1.map(new MapFunction<Tuple4<String, Long, Long, float[][][][]>, float[][][][]>() {
                @Override
                public float[][][][] map(Tuple4<String, Long, Long, float[][][][]> merge1) throws Exception {
                    String str = merge1.f0.replaceAll("\\s+", "");
                    str = str.replace("{", "").replace("}", "");
                    String[] strArr = str.split(",");
                    float[][] arr2D = new float[batchSize][1001];
                    for (int a = 0; a < strArr.length; a++) {
                        int row = a / 1001;
                        int col = a % 1001;
                        arr2D[row][col] = Float.parseFloat(strArr[a].trim());
                    }
                    float[][][][] newdata = new float[batchSize][224][224][3];
                    for (int b = 0; b < batchSize; b++) {
                        int pos = 0;
                        float max = arr2D[b][0];
                        for (int a = 0; a < arr2D[0].length; a++) {
                            if (arr2D[b][a] > max) {
                                max = arr2D[b][a];
                                pos = a;
                            }
                        }
                    //    if (pos == 657 || pos == 480 || pos == 556 || pos == 610 || pos == 628 || pos == 655 || pos == 676 || pos == 706 ||
                      //      pos == 735 || pos == 758 || pos == 780 || pos == 865 || pos == 868) {
                            newdata[b] = merge1.f3[b];
                        //}
                    }
                    return newdata;
                }
            });
            //now we get two data, send to lp

            DataStream<Tuple4<String,Long,Long,float[][][][]>>s3;
            DataStream<Tuple4<String,Long,Long,float[][][][]>>s4;
            if(!Syn) {
                s3=performRequestAsync(PL_URL, data3);
                s4=performRequestAsync(PL_URL, data4);
            }else{
                s3=performRequest(PL_URL, data3);
                s4=performRequest(PL_URL, data4);
            }
            //union

            DataStream<Tuple4<String,Long,Long,float[][][][]>>result1=s3.union(s4);
            DataStream<Tuple3<String,Long,Long>> result=result1.map(new MapFunction<Tuple4<String, Long, Long, float[][][][]>, Tuple3<String, Long, Long>>() {
                @Override
                public Tuple3<String, Long, Long> map(Tuple4<String, Long, Long, float[][][][]> value) throws Exception {
                    return new Tuple3<>(value.f0, value.f1, value.f2);
                }
            });
        }
        // Benchmarking - record timestamp when the scoring is done
        d1.map(new MapFunction<Tuple4<String, Long, Long,float[][][][]>, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(Tuple4<String, Long, Long,float[][][][]> record) throws Exception {
                // filter out failed requests
                if (record.f0 != null) {
                    return new Tuple2<>(record.f1, System.nanoTime());
                } else {
                    return new Tuple2<>(-1L, -1L);
                }
            }
        }).writeAsCsv(outputFile);
        env.execute();
    }

    private static DataStream<Tuple4<String, Long, Long,float[][][][]>> performRequest(String tensorflowURL,
                                                                         DataStream<float[][][][]> batches) {
        DataStream<Tuple4<String, Long, Long,float[][][][]>> results = batches
            .map(new MapFunction<float[][][][], Tuple4<String, Long, Long,float[][][][]>>() {
                @Override
                public Tuple4<String, Long, Long,float[][][][]> map(
                    float[][][][] batch) throws Exception {
                    URL url = new URL(tensorflowURL);
                    long startTime = System.nanoTime();
                    String data = InferenceRequest.buildRequest(batch);
                    String result = InferenceRequest.makeRequest(data, url);
                    // NOTE: Measures the duration for the whole batch
                    return new Tuple4<>(result, startTime, System.nanoTime(),batch);
                }
            });
        return results;
    }
    private static DataStream<Tuple4<String, Long, Long,int[][][][]>> performRequestInt(String tfURL,
                                                                            DataStream<int[][][][]> batches) {
        DataStream<Tuple4<String, Long, Long,int[][][][]>> results = batches
            .map(new MapFunction<int[][][][], Tuple4<String, Long, Long,int[][][][]>>() {
                @Override
                public Tuple4<String, Long, Long,int[][][][]> map(int[][][][] batch) throws Exception {
                    URL url = new URL(tfURL);
                    long startTime = System.nanoTime();
                    String data = InferenceRequest.buildRequest(batch);
                    String result = InferenceRequest.makeRequest(data, url);
                    // NOTE: Measures the duration for the whole batch
                    return new Tuple4<>(result, startTime, System.nanoTime(),batch);
                }
            });
        return results;
    }

    private static DataStream<Tuple4<String, Long, Long,float[][][][]>> performRequestAsync(String tensorflowURL,
                                                                              DataStream<float[][][][]> batches) throws
        Exception {

        AsyncInferenceRequest asyncFunction = new AsyncInferenceRequest(tensorflowURL);
        DataStream<Tuple4<String, Long, Long,float[][][][]>> results = AsyncDataStream.orderedWait(
            batches,
            asyncFunction,
            ASYNC_OPERATOR_TIMEOUT,
            TimeUnit.MILLISECONDS,
            ASYNC_OPERATOR_CAPACITY);
        return results;
    }
    private static DataStream<Tuple4<String, Long, Long,int[][][][]>> performRequestAsyncInt(String tensorflowURL,
                                                                                 DataStream<int[][][][]> batches) throws
        Exception {

        AsyncInferenceRequestInt asyncFunction = new AsyncInferenceRequestInt(tensorflowURL);
        DataStream<Tuple4<String, Long, Long,int[][][][]>> results = AsyncDataStream.orderedWait(
            batches,
            asyncFunction,
            ASYNC_OPERATOR_TIMEOUT,
            TimeUnit.MILLISECONDS,
            ASYNC_OPERATOR_CAPACITY);
        return results;
    }
}
