package util;

import java.util.regex.Pattern;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

public class MangoDBUtil {
	private static Mongo mg = null;
	private static DB db = null;
	private static DBCollection dbConllection = null;
	
	
	/**
	 * 取得mangoDB连接
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static Mongo getMongoDB(){
		try {
			mg = new Mongo("172.16.12.83", 50000);
		}catch (Exception e) {
            e.printStackTrace();
        }
		return mg;
	}
	/**
	 * 对象回收
	 * @param mg
	 * @param db
	 * @param dbConllection
	 */
	public static void destory(Mongo mg,DB db,DBCollection dbConllection) {
        if (mg != null)
            mg.close();
        mg = null;
        db = null;
        dbConllection = null;
    }
	//查询所有数据
	private static void queryAll() {
	    System.out.println("查询users的所有数据：");
	    //db游标
	    DBCursor cur = dbConllection.find();
	    while (cur.hasNext()) {
	    	System.out.println(cur.next());
	    }
	    System.out.println("count:"+cur.count());;
	}
	//添加数据
	public static void add(){
		db = mg.getDB("log");
		DBObject user = new BasicDBObject();
	    user.put("name", "hoojo");
	    user.put("age", 24);
	    System.out.println(dbConllection.save(user));
	}
	//删除数据
	public static void clear(){
		db = mg.getDB("log");
		dbConllection = db.getCollection("log_201511");
		DBObject user = new BasicDBObject();
	    user.put("513", "{id:80383326_20151113152049889, endtime:2015-11-13 15:21:39.421, chargetime:, api_k:513, terminator:user, api_v:0001, hangupmark:200, relcause:19}");
		dbConllection.remove(user);
	}
	//根据条件查找数据
	public static void find(BasicDBObject condition) {  
        DB db = mg.getDB("events");  
        DBCollection collection = db.getCollection("ivrlognew");
        long stime = System.currentTimeMillis();
        System.out.println(stime);
        DBCursor find = collection.find(condition); 
        long etime = System.currentTimeMillis();
        System.out.println(etime);
        System.out.println("总共耗时："+(etime-stime)+"毫秒");
        while (find.hasNext()) {  
        	System.out.println(find.next());
        }  
    }  


	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		MangoDBUtil md = new MangoDBUtil();
		mg = md.getMongoDB();
		//查询所有数据库的名称
		System.out.println(mg.getDatabaseNames());
//		for(String name:db.getCollectionNames()){
//			System.out.println("collection:"+name);
//		}
		
//		db = mg.getDB("log");
//		dbConllection = db.getCollection("log_201511");
//		dbConllection = db.getCollection("logextend_201511");
		
		db = mg.getDB("events");
		dbConllection = db.getCollection("ivrlognew");
		
//		DBCursor cursor = dbConllection.find();
//		while (cursor.hasNext()) {
//            System.out.println(cursor.next());
//        }
//       System.out.println(JSON.serialize(cursor));
        
//        md.add();
//        md.clear();
//        md.queryAll();
		
		find(new BasicDBObject("caller", "15994011574"));
//		find(new BasicDBObject("id", "130382591_20151128181258684"));
		
//		Pattern pattern = Pattern.compile("^02008*", Pattern.CASE_INSENSITIVE);
		
//		find(new BasicDBObject("uuid", pattern));
//		find(new BasicDBObject("api_k", 2222));
	}
}
