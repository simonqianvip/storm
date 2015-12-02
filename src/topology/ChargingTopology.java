package topology;

import java.sql.Connection;
import java.sql.DriverManager;
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
import util.MangoDBUtil;
import util.MapUtil;
import util.OracleManagerUtil;
import util.RedisClient;
import util.TimeUtil;
import util.UuidUtil;
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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class ChargingTopology {

	private BrokerHosts brokerHosts = null;
	private String topic = null;

	public ChargingTopology(String zkHosts, String brokerPath, String topic) {
		this.brokerHosts = new ZkHosts(zkHosts, brokerPath);
		this.topic = topic;
	}

	public static class KafkaInfoToMap extends BaseRichBolt {
		private static final long serialVersionUID = 4296904056487384527L;
		private Log log = LogFactory.getLog(KafkaInfoToMap.class);
		private OutputCollector collector;

		@SuppressWarnings("rawtypes")
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void execute(Tuple input) {
			String line = input.getString(0);
			
			if(line!=null && line.trim().length()!=0){
				@SuppressWarnings("unchecked")
				Map<String, String> map = (Map<String, String>) JSONValue
						.parse(line);
				log.info("json2map="+map);
				String api_k = String.valueOf(map.get("api_k"));
				collector.emit("getCallingInfo", input, new Values(api_k, map));
				// collector.emit("getChargingInfo", input, new Values(map));
//				} 
			}
			collector.ack(input);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declareStream("getCallingInfo", new Fields("api_k", "map"));
//			declarer.declareStream("printChargingInfo", new Fields("printInfo"));
		}
	}

	/**
	 * 处理日志的bolt
	 * @author simon
	 */
	public static class GetCallingInfo extends BaseRichBolt {
		private static final long serialVersionUID = 370370555567887885L;
		@SuppressWarnings("unused")
		private static final Log LOG = LogFactory.getLog(GetCallingInfo.class);
		private OutputCollector collector;
		private Jedis jedis;

		@SuppressWarnings("rawtypes")
		public void prepare(Map stormConf,
				TopologyContext context, OutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void execute(Tuple input) {
			jedis = RedisClient.getJedis();
			@SuppressWarnings("unchecked")
			Map<String, String> map = (Map<String, String>) input.getValue(1);
			// Map map = (Map) input.getValue(0);
			String api_k = String.valueOf(map.get("api_k"));

			// 存储512和516的信息
			if ("512".equals(api_k) || "516".equals(api_k)) {
				String uuid = String.valueOf(map.get("uuid"));
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
						if(caller!=null && caller.trim().length()!=0 && !jedisMap.isEmpty()){
//							LOG.info("512和516 && "+"caller=="+caller+" && jedisMap=="+jedisMap);
							jedis.hmset(caller, jedisMap);
							// 设置key的超时时间为
							jedis.expire(caller, 60 * 60 * 2);
						}
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
				}
			}
			collector.emit(input, new Values(map));
			RedisClient.returnResource(jedis);
			collector.ack(input);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("map"));
		}
	}

	/**
	 * 处理限费的bolt
	 * @author simon
	 */
	public static class GetChargingInfo extends BaseRichBolt {
		private static final long serialVersionUID = 8892974852879834702L;
		private static final Log log = LogFactory.getLog(GetChargingInfo.class);
		private OutputCollector collector;
		private Jedis jedis;
		private static TimeUtil tu = new TimeUtil();
		private Connection conn = null;

		@Override
		public void execute(Tuple input) {
			jedis = RedisClient.getJedis();
			@SuppressWarnings("rawtypes")
			Map map = (Map) input.getValue(0);
			String api_k = String.valueOf(map.get("api_k"));

			if ("512".equals(api_k) || "513".equals(api_k) || "516".equals(api_k)) {
				String id = String.valueOf(map.get("id"));
				if (id.length() != 0) {
					if (jedis.exists(id)) {
						Map<String, String> jedisMap = jedis.hgetAll(id);
						String redis_api_K = jedisMap.get("api_k");
						
						if (!api_k.equals(redis_api_K)) {
							jedis.del(id);
							
							@SuppressWarnings("unchecked")
							Iterator<String> iter = map.keySet().iterator();
							while (iter.hasNext()) {
								String key = iter.next();
								String value = String.valueOf(map.get(key));
								jedisMap.put(key, value);
							}
							String calltag = jedisMap.get("calltag");
							String chargeTime = jedisMap.get("chargetime");
							String endTime = jedisMap.get("endtime");
							log.info("512 513 516 jedisMap == "+jedisMap);
							if ("1".equals(calltag) && chargeTime != null
									&& chargeTime.trim().length() != 0 && endTime.trim().length() != 0) {
								String caller = jedisMap.get("caller");
								String called = jedisMap.get("called");
								String locationum = jedisMap.get("locationum");
								String ctime = tu.formatTime(chargeTime);
								String etime = tu.formatTime(endTime);
								try {
									if(conn != null){
										OracleManagerUtil.getPrepareCall(conn,
												caller, called, ctime, etime,
												locationum);
									}else{
										this.conn = getConnection();
										OracleManagerUtil.getPrepareCall(conn,
												caller, called, ctime, etime,
												locationum);
									}
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
							
						}
					} else {
						HashMap<String, String> newMap = new HashMap<String, String>();
						@SuppressWarnings("unchecked")
						Iterator<String> iter = map.keySet().iterator();
						while (iter.hasNext()) {
							String key = iter.next();
							String value = String.valueOf(map.get(key));
							newMap.put(key, value);
						}
						jedis.hmset(id, newMap);
						jedis.expire(id, 60 * 60 * 2);
					}
				}
			}
			RedisClient.returnResource(jedis);
			collector.emit(input, new Values(map));
			collector.ack(input);
		}
		
		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map arg0, TopologyContext arg1,
				OutputCollector collector) {
			this.collector = collector;
			this.conn = getConnection();
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer outPutFieldsDeclarer) {
			outPutFieldsDeclarer.declare(new Fields("map"));
		}
		//连接oracle数据库
		public static Connection getConnection() {
			Connection con = null;
			String url="jdbc:oracle:thin:@(DESCRIPTION="
					+ "(LOAD_BALANCE=on)"
					+ "(ADDRESS=(PROTOCOL=TCP) (HOST=172.16.64.47)(PORT=1521))"
					+ "(CONNECT_DATA=(SERVICE_NAME=IVRREP)) )";
			try {
				Class.forName("oracle.jdbc.OracleDriver");
				con = DriverManager.getConnection(url, "settle",
						"jsnjivrsettle");
				log.info("*******************************************数据库连接成功！**********************************");
			} catch (Exception e) {
				log.info("******************************连接数据库失败******************************"+ e.getMessage());
				e.printStackTrace();
			}
			return con;
		}
	}
	
	/**
	 * 所有数据拼串存储到mangoDB
	 * @author simon
	 */
	public static class toMangoDB extends BaseRichBolt {
		private static final Log log = LogFactory.getLog(toMangoDB.class);
		private static final long serialVersionUID = 64499411706133149L;
		private static final String LOG = "log_";
		private static final String LOGEXTEND = "logextend_";
		
		private OutputCollector collector;
		private Jedis jedis;
		private Mongo mg;
		private DB db;
		private DBCollection dbConllection;
		
		@SuppressWarnings({ "rawtypes", "unchecked", "deprecation" })

		public void execute(Tuple input) {
			Map<String,String> hashMap = new HashMap<String,String>();
			//redis连接
			jedis = RedisClient.getJedis();
			// TODO mangoDB连接
			mg = MangoDBUtil.getMongoDB("172.16.12.83",50000);
			db = mg.getDB("log");
			
			Map map = (Map) input.getValue(0);
			
			String api_k = String.valueOf(map.get("api_k"));
			String id = String.valueOf(map.get("id"));
			String uuid = String.valueOf(map.get("uuid"));
			
			@SuppressWarnings("unused")
			String select = jedis.select(1);
//			log.info("切换redis 1号库："+select);
			switch (api_k) {
			case "512":
				if(jedis.exists(id)){
					Map<String, String> hgetAll = jedis.hgetAll(id);
					
					Iterator<String> iter = map.keySet().iterator();
					while (iter.hasNext()) {
						String key = iter.next();
						String value = String.valueOf(map.get(key));
						hgetAll.put(key, value);
					}
					save2MangoDB(api_k,uuid,hgetAll);
					jedis.del(id);
				}else{
					HashMap<String, String> newMap = new HashMap<String, String>();
					Iterator<String> iter = map.keySet().iterator();
					while (iter.hasNext()) {
						String key = iter.next();
						String value = String.valueOf(map.get(key));
						newMap.put(key, value);
					}
					jedis.hmset(id, newMap);
					jedis.expire(id, 60 * 60 * 2);
				}
				break;
			case "513":
				String uid = "";
				if(jedis.exists(id)){
					Map<String, String> hgetAll = jedis.hgetAll(id);
					
					Iterator<String> iter = map.keySet().iterator();
					while (iter.hasNext()) {
						String key = iter.next();
						String value = String.valueOf(map.get(key));
						if(key.equals("uuid")){
							uid = value;
						}
						hgetAll.put(key, value);
					}
					save2MangoDB(api_k,uid,hgetAll);
					//删除以id为key的集合
					jedis.del(id);
				}else{
					HashMap<String, String> newMap = new HashMap<String, String>();
					Iterator<String> iter = map.keySet().iterator();
					while (iter.hasNext()) {
						String key = iter.next();
						String value = String.valueOf(map.get(key));
						newMap.put(key, value);
					}
					jedis.hmset(id, newMap);
					jedis.expire(id, 60 * 60 * 2);
				}
				break;
			default:
				save2MangoDB(api_k,uuid,map);
				break;
			}
			MangoDBUtil.destory(mg, db, dbConllection);
			RedisClient.returnResource(jedis);
			collector.ack(input);
		}
		//保存到mangoDb
		private void save2MangoDB(String api_k,String uuid,Map<String, String> hgetAll2) {
			String tableName = UuidUtil.parseUUID(uuid);
			if("512".equals(api_k) || "513".equals(api_k)){
				dbConllection = db.getCollection(LOG+tableName);
			}else{
				dbConllection = db.getCollection(LOGEXTEND+tableName);
			}
			DBObject obj = new BasicDBObject();
			obj.putAll(hgetAll2);
			dbConllection.save(obj);
		}

		@SuppressWarnings("rawtypes")
		public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
			this.collector = arg2;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
		}
	}

	private StormTopology builTopology() {
		SpoutConfig spoutConf = new SpoutConfig(this.brokerHosts, this.topic,
				"/kafka", "charging_topology");
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

		TopologyBuilder builder = new TopologyBuilder();
		/**
		 * spout线程数不能大于kafka的分区数
		 */
		builder.setSpout("logs", new KafkaSpout(spoutConf), 5);

		builder.setBolt("toMap", new KafkaInfoToMap(), 10).shuffleGrouping("logs");

		builder.setBolt("callingInfo", new GetCallingInfo(), 10).fieldsGrouping(
				"toMap", "getCallingInfo", new Fields("api_k"));
		// builder.setBolt("callingInfo", new GetCallingInfo(), 10)
		// .shuffleGrouping("toMap", "getChargingInfo");

		builder.setBolt("charingInfo", new GetChargingInfo(),10).shuffleGrouping("callingInfo");
		 
		builder.setBolt("toMangoDB", new toMangoDB(),10).shuffleGrouping("charingInfo");

//		builder.setBolt("print", new Print(), 10).shuffleGrouping("toMap",
//				"printChargingInfo");

		return builder.createTopology();
	}

	public static void main(String[] args) throws Exception {
		String zkHosts = "172.16.12.84:2181,172.16.12.87:2181";
		String brokerPath = "/kafka/brokers";
		// topic名称
		String topic = "ivr_topic";
		ChargingTopology callingTopology = new ChargingTopology(zkHosts,
				brokerPath, topic);
		StormTopology stormTopology = callingTopology.builTopology();
		Config config = new Config();
		// 集群上
		if (args != null && args.length > 0) {
			String name = args[0];
			//设置spout的最大缓存数，超过10万条就不再拉取数据了
			config.setMaxSpoutPending(100000);
			config.setNumWorkers(2);
			config.setDebug(true);
			config.setNumAckers(2);
			try {
				StormSubmitter.submitTopology(name, config, stormTopology);
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				e.printStackTrace();
			}
		} else {
			// 本地
			List<String> zkServerList = new ArrayList<String>();
			zkServerList.add("172.16.12.84");
			zkServerList.add("172.16.12.87");
			config.put(Config.STORM_ZOOKEEPER_SERVERS, zkServerList);
			config.put(Config.STORM_ZOOKEEPER_PORT, 2181);

			config.setNumWorkers(1);
			config.setNumAckers(2);
//			config.setMaxSpoutPending(1000000);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("chargingInfo", config, stormTopology);
//			Thread.sleep(60000);
//			localCluster.shutdown();
		}
	}
}
