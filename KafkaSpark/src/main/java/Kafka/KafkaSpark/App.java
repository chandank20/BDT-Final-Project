package Kafka.KafkaSpark;

import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
	static String lastKey="";
	   static List<String> list=new ArrayList<String>();
	   public static void main(String[] args) throws Exception 
	   {      
		   MySpark spark=new MySpark();
		   SparkResources resources = new SparkResources();
		   SparkKafkaConsumer consumer = new SparkKafkaConsumer(resources);
		   consumer.Wait(()-> {
					try {
						Thread.sleep(3000);
					} catch (Exception e) {
						e.printStackTrace();
					}
					return true;
				}
		   , (String key, String val,boolean new_key)-> 
		   {
			   if(lastKey.isEmpty())
				   lastKey=key;
			   if(new_key)
			   {
				   try 
				   {
					   if(!list.isEmpty())
						   spark.SparkProcess(lastKey, list);
				   } 
				   catch (Exception e) { 
					   e.printStackTrace();
				   }
				   
				   lastKey=key;
				   list.clear();
			   }
			 
			   list.add(val);
		   });
	   }
}
