package topology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONValue;

import redis.clients.jedis.Jedis;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import util.RedisClient;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CallingTopology {

	private BrokerHosts brokerHosts = null;
	private String topic = null;

	public CallingTopology(String zkHosts, String brokerPath, String topic) {
		this.brokerHosts = new ZkHosts(zkHosts, brokerPath);
		this.topic = topic;
	}

	public static class KafkaInfoToMap extends BaseRichBolt {
		private static final long serialVersionUID = 4296904056487384527L;
		private static Log LOG = LogFactory.getLog(KafkaInfoToMap.class);
		private OutputCollector collector;

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void execute(Tuple input) {
			String line = input.getString(0);
			Map<String, String> parse = (Map<String, String>) JSONValue
					.parse(line);
			String api_k = String.valueOf(parse.get("api_k"));
			if (api_k.equals("512") || api_k.equals("516")) {
				String uuid = parse.get("uuid");
				collector
						.emit("getCallingInfo", input, new Values(uuid, parse));
			} else {
				collector.emit("printCallInfo", input, new Values(parse));
			}
			collector.ack(input);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declareStream("getCallingInfo", new Fields("uuid", "map"));
			declarer.declareStream("printCallInfo", new Fields("printInfo"));
		}

	}

	public static class GetCallingInfo extends BaseRichBolt {
		private static final long serialVersionUID = 370370555567887885L;
		private static Log LOG = LogFactory.getLog(GetCallingInfo.class);
		private OutputCollector collector;
		private static RedisClient redis;
		private Jedis jedis;

		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;
			this.redis = new RedisClient();
		}

		@Override
		public void execute(Tuple input) {
			@SuppressWarnings("rawtypes")
			Map map = (Map) input.getValue(1);
			jedis = redis.getJedis();
			String uuid = String.valueOf(map.get("uuid"));
			String api_k = String.valueOf(map.get("api_k"));
			if (jedis.exists(uuid)) {
				Map<String, String> jedisMap = jedis.hgetAll(uuid);
				String redis_api_K = jedisMap.get("api_k");
				if (!api_k.equals(redis_api_K)) {
					jedis.del(uuid);
					Iterator<String> iter = map.keySet().iterator();
					while (iter.hasNext()) {
						String key = iter.next();
						String value = String.valueOf(map.get(key));
						jedisMap.put(key, value);
					}
					String caller = jedisMap.get("caller");
					jedis.hmset(caller, jedisMap);
					//设置key的超时时间为
					jedis.expire(caller, 60 * 60 * 2);
					// collector.emit(input, new Values(jedisMap));
				}

			} else {
				HashMap<String, String> newMap = new HashMap<String, String>();
				Iterator<String> iter = map.keySet().iterator();
				while (iter.hasNext()) {
					String key = iter.next();
					String value = String.valueOf(map.get(key));
					newMap.put(key, value);
				}
				jedis.hmset(uuid, newMap);
				jedis.expire(uuid, 60 * 60 * 2);
				// collector.emit(input, new Values(newMap));
			}

			this.redis.returnResource(jedis);
			collector.ack(input);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("map"));
		}

	}

	public static class Print extends BaseRichBolt {
		private static Log LOG = LogFactory.getLog(Print.class);
		private static final long serialVersionUID = 64499411706133149L;
		private OutputCollector collector;

		@Override
		public void execute(Tuple arg0) {
			List<Object> list = arg0.getValues();
			collector.ack(arg0);
		}

		@Override
		public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
			this.collector = arg2;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			arg0.declare(new Fields("result_map"));
		}

	}

	private StormTopology builTopology() {
		SpoutConfig spoutConf = new SpoutConfig(this.brokerHosts, this.topic,
				"/kafka", "kafka_storm");
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("ivr_logs", new KafkaSpout(spoutConf),10);
		builder.setBolt("toMap", new KafkaInfoToMap(),15).shuffleGrouping(
				"ivr_logs");
		builder.setBolt("CallingInfo", new GetCallingInfo(),20).fieldsGrouping(
				"toMap", "getCallingInfo", new Fields("uuid"));
		builder.setBolt("print", new Print(),10).shuffleGrouping("toMap",
				"printCallInfo");

		return builder.createTopology();
	}

	public static void main(String[] args) throws Exception {
		String zkHosts = "JSNJ-IVR-SRV-I620G10-22:2181,JSNJ-IVR-SRV-I620G10-23:2181,JSNJ-IVR-SRV-I620G10-24:2181";
		String brokerPath = "/kafka/brokers";
		// TODO topic名称
		String topic = "IVR";
		CallingTopology callingTopology = new CallingTopology(zkHosts,
				brokerPath, topic);
		StormTopology stormTopology = callingTopology.builTopology();
		Config config = new Config();
		//集群环境下
		if (args != null && args.length > 0) {
			String name = args[0];
			config.setNumWorkers(4);
//			config.setDebug(true);
			config.setNumAckers(2);
			try {
				StormSubmitter.submitTopology(name, config, stormTopology);
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				e.printStackTrace();
			}
		} else {
			//设置ZK的信息
			List<String> zkServerList = new ArrayList<String>();
			zkServerList.add("JSNJ-IVR-SRV-I620G10-22");
			zkServerList.add("JSNJ-IVR-SRV-I620G10-23");
			zkServerList.add("JSNJ-IVR-SRV-I620G10-24");
			config.put(Config.STORM_ZOOKEEPER_SERVERS, zkServerList);
			config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
			config.setDebug(false);

			config.setNumWorkers(1);
			config.setNumAckers(5);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("callingInfo", config, stormTopology);
			Thread.sleep(60000);
			localCluster.shutdown();
		}
	}
}
