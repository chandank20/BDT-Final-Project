import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;




public class TwitterDataSender {
	 private final static String BOOTSTRAP_SERVERS ="localhost:9092";
	    private final static String TOPIC = "tweet-topic";
	    
	    public static Producer<String, String> createProducer() {
	        Properties props = new Properties();
	        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
	        props.put("key.serializer", StringSerializer.class.getName());
	        props.put("value.serializer", StringSerializer.class.getName());
	        return new KafkaProducer<String, String>(props);
	    }
	    
	    public void sendTwitterData(String id, String text) throws Exception {
	        final Producer<String, String> producer = createProducer();
	        final ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, id, text);
	        producer.send(record).get();
	    }
}
