package util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

public class MangoDBUtil {
	private static Log log = LogFactory.getLog(MangoDBUtil.class);
	private static final String IP = "172.16.12.83";
	private static final int PORT = 50000;
	private static Mongo mg = null;
	private static DB db = null;
	private static DBCollection dbConllection = null;

	/**
	 * 取得mangoDB连接
	 * 
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static Mongo getMongoDB(String ip, int port) {
		try {
			mg = new Mongo(ip, port);
			log.info("【 mongoDB connection server success】");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return mg;
	}

	/**
	 * 连接mongoDb的数据库与集合
	 * 
	 * @param dbName
	 * @param collections
	 * @return
	 */
	public static DBCollection getDBConllection(String dbName,
			String collections) {
		mg = getMongoDB(IP, PORT);
		getDatabase(dbName);
		dbConllection = db.getCollection(collections);
		log.info("【 connection " + dbName + "." + collections + " success】");
		return dbConllection;
	}

	/**
	 * 与库取的连接
	 * 
	 * @param dbName
	 */
	public static DB getDatabase(String dbName) {
		log.info("【connection " + dbName + " success 】");
		return db = mg.getDB(dbName);
	}

	/**
	 * 对象回收
	 * 
	 * @param mg
	 * @param db
	 * @param dbConllection
	 */
	public static void destory(Mongo mg, DB db, DBCollection dbConllection) {
		if (mg != null)
			mg.close();
		mg = null;
		db = null;
		dbConllection = null;
		log.info("【close " + db + "." + dbConllection + " 】");
	}

	// 查询所有数据
	private static void queryAll() {
		log.info("【查询users的所有数据：】");
		// db游标
		DBCursor cur = dbConllection.find();
		while (cur.hasNext()) {
			log.info(cur.next());
		}
		log.info("【 count:" + cur.count() + " 】");
	}

	// 添加数据
	public static void addData() {
		DBObject user = new BasicDBObject();
		user.put("name", "hoojo");
		user.put("age", 24);
		WriteResult result = dbConllection.save(user);
		log.info("【 save success , result is " + result + " 】");
	}

	// 删除数据
	public static void clear(DBCollection dbConllection) {
		DBObject user = new BasicDBObject();
		user.put(
				"513",
				"{id:80383326_20151113152049889, endtime:2015-11-13 15:21:39.421, chargetime:, api_k:513, terminator:user, api_v:0001, hangupmark:200, relcause:19}");
		dbConllection.remove(user);
	}

	// 根据条件查找数据
	public static void find(String dbName, String collections,
			BasicDBObject condition) {
		DBCollection conllection = MangoDBUtil.getDBConllection(dbName,
				collections);
		DBCursor find = conllection.find(condition);
		while (find.hasNext()) {
			log.info(find.next());
		}
	}

	/**
	 * 显示所有数据库的集合（表名）
	 */
	public static void showAllCollection(String dbName) {
		mg = MangoDBUtil.getMongoDB(IP, PORT);
		db = MangoDBUtil.getDatabase(dbName);
		for (String name : db.getCollectionNames()) {
			log.info("collection:" + name);
		}
	}

	/**
	 * 显示所有数据库名称
	 */
	public static void showAllDatabase() {
		mg = MangoDBUtil.getMongoDB(IP, PORT);
		log.info(mg.getDatabaseNames());
	}

	/**
	 * 展示集合里每条记录
	 */
	public static void showInfo(String dbName, String Collections) {
		dbConllection = MangoDBUtil.getDBConllection(dbName, Collections);
		DBCursor cursor = dbConllection.find();
		while (cursor.hasNext()) {
			log.info(cursor.next());
		}
		log.info(JSON.serialize(cursor));
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		//dataBase:events,log table:log_201511,logextend_201511,ivrlognew
		// DBCollection dbc = MangoDBUtil.getDBConllection("log", "log_201511");
		/*
		 * caller id uuid api_k
		 */
		CountTimeUtil.countTime(new CallBack() {
			@Override
			public void execute() {
				find("events", "ivrlognew", new BasicDBObject("caller",
						"15994011574"));
			}
		});
	}
}
