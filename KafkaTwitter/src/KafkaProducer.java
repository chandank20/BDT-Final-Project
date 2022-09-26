import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;

import com.google.gson.Gson;


public class KafkaProducer {

	static final String BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAHCMhQEAAAAARB4Uvh3mFMe120F%2Bpql%2BNk8YD4o%3D176pOZheQXzSTB1pwsEzyYTuWzbyUZRorEeRRjUmGiVdhBQvaW";


	public static void main(String[] args) throws Exception {
		
		String bearerToken = BEARER_TOKEN;
		if (null != bearerToken) {
			getStreamData(bearerToken);
		} else {
			System.out.println("Problem to getting BEARER_TOKEN. Please, check the environment variable.");
		}
	}

	private static void getStreamData(String bearerToken) throws Exception {
		
		HttpClient httpClient = HttpClients
				.custom()
				.setDefaultRequestConfig(RequestConfig.custom()
				.setCookieSpec(CookieSpecs.STANDARD).build())
				.build();

		URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/sample/stream");

		HttpGet httpGet = new HttpGet(uriBuilder.build());
		httpGet.setHeader("Authorization",String.format("Bearer %s", bearerToken));

		HttpResponse response = httpClient.execute(httpGet);
		HttpEntity entity = response.getEntity();
		TwitterDataSender twitterDataSender = new TwitterDataSender();
		if (null != entity) {

			BufferedReader reader = new BufferedReader(new InputStreamReader((entity.getContent())));
			String line = reader.readLine();
			Gson gson = new Gson();
			while (line != null) {
				System.out.println(line);
				Twitter body = gson.fromJson(line, Twitter.class);
				
				if (body !=null ){
					String id = body.getData().getId();
					String text = body.getData().getText();
					twitterDataSender.sendTwitterData(id, text);
				}
				line = reader.readLine();
			}
		}

	}
}
