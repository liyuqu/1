package Request;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

public class InferenceRequest {

    public static String makeRequest(String request, URL url) throws Exception {
        // Establishing connection
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        // Writes the data as an output stream
        OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
        writer.write(request);
        writer.flush();

        // Receiving and parsing the output
        if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) { // success

            // Uses a buffered reader to parse the input stream
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    connection.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            String predictionStr = response.toString();

            predictionStr = predictionStr.contains("predictions") ? predictionStr.replace("[", "").replace("]", "")
                    .replace("\"predictions\":",
                            "") : null;

            // Outputting the edges and inference to the DataStream
            return predictionStr;
        } else { //failed
            System.out.println("POST request failed! " + connection.getResponseCode());
        }
        return null;
    }

    public static String buildRequest(float[][][][] inputBatch) throws Exception {
        String data = "{\"signature_name\": \"serving_default\", \"instances\": " + Arrays.deepToString(inputBatch) + "}";
        return data;
    }
    public static String buildRequest(int[][][][] inputBatch) throws Exception {
        String data = "{\"signature_name\": \"serving_default\", \"instances\": " + Arrays.deepToString(inputBatch) + "}";
        return data;
    }
}
