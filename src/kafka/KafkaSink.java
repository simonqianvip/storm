package kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

public class KafkaSink extends AbstractSink implements Configurable {
	private static final Log logger = LogFactory.getLog(KafkaSink.class);

	private String topic;
	private Producer<String, String> producer;

	public void configure(Context context) {
		topic = "myTopic";
		Properties props = new Properties();
		props.setProperty(
				"metadata.broker.list",
				"JSNJ-IVR-SRV-I620G10-22:9092,JSNJ-IVR-SRV-I620G10-23:9092,JSNJ-IVR-SRV-I620G10-24:9092");
		props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "kafka.Partitionertest");
		props.put(
				"zookeeper.connect",
				"JSNJ-IVR-SRV-I620G10-22:2181,JSNJ-IVR-SRV-I620G10-23:2181,JSNJ-IVR-SRV-I620G10-24:2181/kafka");
//		props.setProperty("num.partitions", "3"); //
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
		logger.info("KafkaSink��ʼ�����.");
	}

	@Override
	public Status process() throws EventDeliveryException {
		Channel channel = getChannel();
		Transaction tx = channel.getTransaction();
		try {
			tx.begin();
			Event e = channel.take();
			if (e == null) {
				tx.rollback();
				return Status.BACKOFF;
			}
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					topic, new String(e.getBody()));
			producer.send(data);
			logger.info("flume��kafka������Ϣ��" + new String(e.getBody()));
			tx.commit();
			return Status.READY;
		} catch (Exception e) {
			logger.error("Flume KafkaSinkException:", e);
			tx.rollback();
			return Status.BACKOFF;
		} finally {
			tx.close();
		}
	}
}
