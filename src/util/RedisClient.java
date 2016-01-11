package util;

import java.util.List;

import org.apache.commons.logging.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisClient {
	private static Log log = LogFactory.getLog(RedisClient.class);
	//172.16.12.96    172.16.0.147
	private static String ADDR = "172.16.12.96";
	private static int PORT = 6379;
	private static int MAX_ACTIVE = 5000;
	private static int MAX_IDLE = 200;
	private static int MAX_WAIT = 10000;
	private static int TIMEOUT = 10000;
	private static boolean TEST_ON_BORROW = true;
	private static String AUTH = "gyredis";
	private static JedisPool jedisPool = null;
	
	/**
	 * redis初始化
	 */
	static{
		initjedis();
	}
	/**
	 * 获取redis连接池
	 */
	public static void initjedis() {
		try {
			JedisPoolConfig config = new JedisPoolConfig();
			config.setMaxTotal(MAX_ACTIVE);
			config.setMaxIdle(MAX_IDLE);
			config.setMaxWaitMillis(MAX_WAIT);
			config.setTestOnBorrow(TEST_ON_BORROW);
			jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT,AUTH);
			log.info("【init jedis,Get jedisPool success】");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取redis对象
	 * @return
	 */
	public synchronized static Jedis getJedis() {
		try {
			Jedis jedis = null;
			if (jedisPool != null) {
				jedis = jedisPool.getResource();
				if(jedis!=null){
					log.info("【 get redis resource success 】");
				}else{
					log.error("【 get redis resource fail 】");
				}
				return jedis;
			}else{
				initjedis();
				jedis = jedisPool.getResource();
				return jedis;
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.info("【get redis is error 】"+e.getMessage());
			return null;
		}
	}

	/**
	 * 释放redis连接
	 * @param jedis
	 */
	public static void returnResource(final Jedis jedis) {
		if (jedis != null) {
			jedisPool.returnResource(jedis);
			log.info("【 return redis resource 】");
		}
	}
//	public static void main(String[] args) {
//		RedisClient.getJedis();
//		
//	}

}
