package topology;

import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;

public class CountTopology {

	private BrokerHosts brokerHosts = null;
	private String topic = null;
	
	public CountTopology(String zkHosts, String brokerPath, String topic) {
		this.brokerHosts = new ZkHosts(zkHosts, brokerPath);
		this.topic = topic;
	}

	public static void main(String[] args) {

	}

}
