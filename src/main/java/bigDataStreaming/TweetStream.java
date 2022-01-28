package bigDataStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

public class TweetStream {

    String key = "xF6WElcY90sc3XQunUINZsoYC";
    String keySecret = "Tf2Sj4cM4AFYgLSnZAzQqhCKb1tqQihMogkLELl5lZwewbhRJy";
    String accessToken = "1469028155361054725-7gvH4bU87NrCOilDijgZBZOhxrSAVh";
    String accessTokenSecret = "euSB36d7mF7OBXKxO26O4oM31MeeD0SKUIu044pG4csoE";


    public static void main(String[] args) {

        try {
            Logger.getLogger("org").setLevel(Level.OFF);  //Permet de masquer les logs spark qui ne sont pas pertinant.
            // On visualise directement les tweet reçus

            TweetStream ts = new TweetStream();
            //ts.setAccessToken();

            //Configuration du cluster
            SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Twitter_spark_streaming");
            // "setMaster("local[2]")" => pour dire que le programme sera lancé en local en utilisant 2 threads

            JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(15000));

            //Affectation des paramètres d'authentification
            System.setProperty("twitter4j.oauth.key",ts.getKey());
            System.setProperty("twitter4j.oauth.keySecret",ts.getKeySecret());
            System.setProperty("twitter4j.oauth.accessToken",ts.getAccessToken());
            System.setProperty("twitter4j.oauth.accessTokenSecret",ts.getAccessTokenSecret());


            //On commence ici à recevoir les flux twitter (Sans flitre)
            JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jsc); /*On aura le flux directement
             * collecter via l'api twitter
             * qui contient les tweets avec
             * les meta données
             */

            final JavaDStream<String> status = twitterStream.map((Function<Status, String>) v1 -> v1.getText());

            /*Avant d'afficher via <<status.print>>, nous allons parcourir les tweets reçus et extraire seulement le text ou
             * le corps du tweet à partir de <<v1.getText()>>
             */

            status.print();


            jsc.start();
            jsc.awaitTermination();  /* Permet de dire au programme de ne pas faire un stop et d'attendre que tous
             * les excécuteurs finissent leurs taches avant d'arrêter le programme.
             */
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getKeySecret() {
        return keySecret;
    }

    public void setKeySecret(String keySecret) {
        this.keySecret = keySecret;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public String getAccessTokenSecret() {
        return accessTokenSecret;
    }

    public void setAccessTokenSecret(String accessTokenSecret) {
        this.accessTokenSecret = accessTokenSecret;
    }
}
