package util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import com.google.gson.Gson;

public class DateUtil {
	/**
	 * 日期转换成秒
	 * @param str
	 * @return double
	 * @throws Exception
	 * @author simon
	 * @date 2015年6月11日 下午1:20:08
	 */
	public static double dateChangeSecond(String str) throws Exception{
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = sdf.parse(str);
		long time = date.getTime();
		return time/1000;
	}
	/**
	 * 秒向上取整
	 * 例子：61秒 = 120秒；
	 * @param num
	 * @return
	 * @author simon
	 * @date 2015年6月11日 下午1:27:43
	 */
	public static double secondCeil(double num){
		if(num != 0){
			return Math.ceil(num/60)*60;
		}
		return num;
	}
	/**
	 * 日期转换成String
	 * @param time
	 * @return 字符串
	 * @author simon
	 * @date 2015年6月11日 下午1:37:12
	 */
	public static String dateChangeString(Date time){
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");  
		java.util.Date date=new java.util.Date();  
		String str=sdf.format(time); 
		return str;
	}
	/**
	 * 返回拨打实际时间
	 * @param start
	 * @param end
	 * @return
	 * @throws Exception
	 * @author simon
	 * @date 2015年6月11日 下午1:47:11
	 */
	public static double ticketTime(Date start,Date end) throws Exception{
		double s = DateUtil.dateChangeSecond(DateUtil.dateChangeString(start));
		double e = DateUtil.dateChangeSecond(DateUtil.dateChangeString(end));
		return e-s;
	}
	
	/**
	 * json转换成map
	 * @param jsonStr
	 * @return
	 * @author simon
	 * @date 2015年6月16日 上午10:06:17
	 */
	public static Map<String, String> jsonToMap(String jsonStr) { 
		Map<String, String> ObjectMap = null; 
		Gson gson = new Gson(); 
		java.lang.reflect.Type type = new com.google.gson.reflect.TypeToken<Map<?,?>>() {}.getType(); 
		ObjectMap = gson.fromJson(jsonStr, type); 
		return ObjectMap; 
	}
	
	public static <T> String getType(T t) {
		if (t instanceof String) {
			return "string";
		} else if (t instanceof Long) {
			return "long";
		} else {
			return " do not know";
		} 
	}
	
}
