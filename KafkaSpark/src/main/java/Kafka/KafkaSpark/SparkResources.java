package Kafka.KafkaSpark;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SparkResources {
	public String TOPIC;
	public String PORT;
	
	public SparkResources() throws IOException
	{
		InputStream inputStream = null;
		try 
		{
			Properties prop = new Properties();
			inputStream = getClass().getClassLoader().getResourceAsStream("config.properties");
			if (inputStream != null) 
			{
				prop.load(inputStream);
				this.TOPIC=prop.getProperty("TOPIC");
				this.PORT=prop.getProperty("PORT");
			}
		} 
		catch (Exception ex) 
		{ } 
		finally 
		{
			if (inputStream != null)
				inputStream.close();
		}
	}

}
