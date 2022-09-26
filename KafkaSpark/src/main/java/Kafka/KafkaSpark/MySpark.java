package Kafka.KafkaSpark;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MySpark {
	static int counter = 1;
	private JavaSparkContext sc;
	private SparkHBase db;
	private HashMap<String,String> mapKeywords;
	
	public MySpark() throws IOException
	{
		this.sc = new JavaSparkContext("local[*]","MySpark",new SparkConf());
		this.db = new SparkHBase();
		this.mapKeywords = this.db.GetKeywords();
	}
	
	public void SparkProcess(String key, List<String> l) throws IOException
	{
		JavaRDD<String> list = this.sc.parallelize(l).flatMap(line -> Arrays.asList(line.toUpperCase().split("RT"))).filter(line ->!line.isEmpty());
		JavaPairRDD<String,Tweet>  tweetResults=list.mapToPair(new SparkPairFunction(key,this.mapKeywords)).sortByKey();
		
		this.db.AddTwitterData(key,tweetResults.values());
	}
	
	static class SparkPairFunction implements PairFunction<String, String, Tweet>,Serializable
	{
		private String key;
		private HashMap<String,String> hMap = new HashMap<String,String>();
		
		public SparkPairFunction(String k,HashMap<String,String> m)
		{
			this.key = k;
			this.hMap = m;
		}
		
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Tweet> call(String _line) throws Exception 
		{
			String line=_line.toLowerCase();
			Tweet tweet = new Tweet();
			if(line.contains("@") && line.contains(":"))
			{
				int pos1=line.indexOf("@");
				int pos2=line.indexOf(":");
				tweet.user=line.substring(pos1, pos2);
			}
			
			for(String x:this.hMap.keySet())
			{
				for(String s:this.hMap.get(x).split(","))	
				{
					if(line.contains(s.toLowerCase()))
					{
						tweet.keyword.add(new Tuple2<String,String>(x,s));
					}
				}
			}
			if(tweet.keyword.isEmpty())
			{
				tweet.keyword.add(new Tuple2<String,String>("General",""));
			}
			return new Tuple2<String,Tweet>(this.key,tweet);
		}
		
	}

}
