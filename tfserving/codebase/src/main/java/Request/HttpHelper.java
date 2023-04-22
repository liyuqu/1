package Request;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.List;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.mime.FileBody;
import org.apache.hc.client5.http.entity.mime.InputStreamBody;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpHelper {

    public static Logger logger = LoggerFactory.getLogger(HttpHelper.class);

    public static String sendImages(List<String> request, String url) throws Exception {
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        // Writes the data as an output stream
        OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
        writer.write(request.toString());
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
}
