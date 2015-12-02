package util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sun.xml.internal.bind.v2.TODO;

import redis.clients.jedis.Jedis;

public class RedisUtil {
	private static RedisClient rc = new RedisClient();
	/**
	 * 返回当前redis库中，key的数目
	 */
	public static void showResult() {
		int count = 0;
		Jedis jedis = rc.getJedis();
		jedis.select(1);
		Long size = jedis.dbSize();
		System.out.println("key的个数=" + size);
		
		if(size>1){
			System.out.println("显示redis库里的所有key******************");
			Set<String> keys = jedis.keys("*");
			Iterator<String> it = keys.iterator();
			while (it.hasNext()) {
				String key = it.next();
//				Pattern pattern = Pattern.compile("[0-9]*");
//				Matcher isNum = pattern.matcher(key);
//				if(isNum.matches()) {
//					System.out.println(key);
//					count ++;
					showMap(key);
//				}
			}
		}
		System.out.println("总共有"+count+"个匹配的记录");
		rc.returnResource(jedis);
	}

	/**
	 * 清空当前redis库
	 */
	public static void clear() {
		Jedis jedis = rc.getJedis();
		rc.returnResource(jedis);
		String db = jedis.flushDB();
		System.out.println("数据库清空 == "+db);
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
			System.out.println("{" + entry.getKey() + " = "
					+ entry.getValue()+" }");
		}
		rc.returnResource(jedis);
	}
	
	private static void add(){
		Jedis jedis = rc.getJedis();
		jedis.select(1);
		HashMap<String, String> hashMap = new HashMap<>();
//		hashMap.put("name", "zhangsan");
//		hashMap.put("age", "18");
		hashMap.put("age1", "19");
		hashMap.put("age2", "20");
		jedis.hmset("222", hashMap);
		
	}

	public static void main(String[] args) {
//		add();
//		RedisUtil.showResult();
		 showMap("222");
//		RedisUtil.showMap("2a024ab3_20151116155826063");
//		RedisUtil.clear();
	}

}
