package util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;

public class RedisUtil {
	private static Log log = LogFactory.getLog(RedisUtil.class);
	private static RedisClient rc = new RedisClient();
	/**
	 * 返回当前redis库中，key的数目
	 */
	public static void showResult() {
		Jedis jedis = rc.getJedis();
		jedis.select(1);
		Long size = jedis.dbSize();
		log.info("key的个数=" + size);
		log.error("key的个数=" + size);
		if(size>=1){
			System.out.println("显示redis库里的所有key******************");
			Set<String> keys = jedis.keys("*");
			Iterator<String> it = keys.iterator();
			while (it.hasNext()) {
				String key = it.next();
//				Pattern pattern = Pattern.compile("[0-9]*");
//				Matcher isNum = pattern.matcher(key);
//				if(isNum.matches()) {
					log.info(key);
					showMap(key);
//				}
			}
		}
		rc.returnResource(jedis);
	}

	/**
	 * 清空当前redis库
	 */
	public static void clear() {
		Jedis jedis = rc.getJedis();
		rc.returnResource(jedis);
		String db = jedis.flushDB();
		log.info("数据库清空 == "+db);
	}

	@SuppressWarnings({ "unused", "unchecked" })
	private static void HashOperate(String mapName, @SuppressWarnings("rawtypes") Map map) {
		rc.getJedis().hmset(mapName, map);
		rc.getJedis().expire(mapName, 60 * 30);
		rc.returnResource(rc.getJedis());
	}
	/**
	 * 根据key展示对应的值
	 * @param key
	 */
	private static void showMap(String key) {
		Jedis jedis = rc.getJedis();
		jedis.select(1);
		Map<String, String> map = jedis.hgetAll(key);
		for (Entry<String, String> entry : map.entrySet()) {
			log.info("{" + entry.getKey() + " = "
					+ entry.getValue()+" }");
		}
		rc.returnResource(jedis);
	}
	
	private static void add(){
		Jedis jedis = rc.getJedis();
		jedis.select(1);
		HashMap<String, String> hashMap = new HashMap<>();
		hashMap.put("age1", "19");
		hashMap.put("age2", "20");
		jedis.hmset("222", hashMap);
		
	}

	public static void main(String[] args) {
//		add();
		RedisUtil.showResult();
//		 showMap("222");
//		RedisUtil.showMap("2a024ab3_20151116155826063");
//		RedisUtil.clear();
	}

}
