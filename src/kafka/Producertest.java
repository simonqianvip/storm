package kafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class Producertest {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("zk.connect", "JSNJ-IVR-SRV-I620G10-22:2181,JSNJ-IVR-SRV-I620G10-23:2181,JSNJ-IVR-SRV-I620G10-24:2181/kafka");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list",
				"JSNJ-IVR-SRV-I620G10-22:9092,JSNJ-IVR-SRV-I620G10-23:9092,JSNJ-IVR-SRV-I620G10-24:9092");
		 props.put("partitioner.class", "kafka.Partitionertest");
		props.put("request.required.acks", "1");
//		props.put("num.partitions", "4");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		for (int i = 0; i < 10; i++) {
			SimpleDateFormat formatter = new SimpleDateFormat(
					"yyyy年MM月dd日 HH:mm:ss SSS");
			Date curDate = new Date(System.currentTimeMillis());
			String str = formatter.format(curDate);
			String msg = "idoall.org " + i + " = " + str;
			String key = i + "";
			producer.send(new KeyedMessage<String, String>("myTopic",
					key, msg));
		}
		producer.close();
	}
}
