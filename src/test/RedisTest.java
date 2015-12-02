package test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import util.RedisClient;

public class RedisTest {
	public static void main(String[] args) {
		RedisClient redis = new RedisClient();
		
		Jedis jedis = redis.getJedis();
		
//		String select = jedis.select(2);
//		System.out.println("select:"+select);
		Map map = new HashMap<>();
		// TODO 512,513,515,516,517,518存在这里
		map.put("512", "{uuid=123|name=123|ip=123.456.789.1}");
		
		// TODO 存入按键详情和语音信息
		map.put("keyPress", "{name:110|ip:123.456.789.2;name:120|ip:123.234.345.2}");
		
		//插入到map集合中
		jedis.hmset("id-uuid", map);
		
		Map<String, String> map2 = jedis.hgetAll("id-uuid");
		System.out.println("map2=="+map2);
		
		
		// TODO 取出hash中key的指定map中的键的值
		List<String> keyPress = jedis.hmget("id-uuid", "keyPress");
		System.out.println(keyPress);
		
		
		Long size = jedis.dbSize();
		System.out.println("key的个数=" + size);
	}

}
