package util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import com.google.gson.Gson;

public class DateUtil {
	/**
	 * ����ת������
	 * @param str
	 * @return double
	 * @throws Exception
	 * @author simon
	 * @date 2015��6��11�� ����1:20:08
	 */
	public static double dateChangeSecond(String str) throws Exception{
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = sdf.parse(str);
		long time = date.getTime();
		return time/1000;
	}
	/**
	 * ������ȡ��
	 * ���ӣ�61�� = 120�룻
	 * @param num
	 * @return
	 * @author simon
	 * @date 2015��6��11�� ����1:27:43
	 */
	public static double secondCeil(double num){
		if(num != 0){
			return Math.ceil(num/60)*60;
		}
		return num;
	}
	/**
	 * ����ת����String
	 * @param time
	 * @return �ַ�
	 * @author simon
	 * @date 2015��6��11�� ����1:37:12
	 */
	public static String dateChangeString(Date time){
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");  
		java.util.Date date=new java.util.Date();  
		String str=sdf.format(time); 
		return str;
	}
	/**
	 * ���ز���ʵ��ʱ��
	 * @param start
	 * @param end
	 * @return
	 * @throws Exception
	 * @author simon
	 * @date 2015��6��11�� ����1:47:11
	 */
	public static double ticketTime(Date start,Date end) throws Exception{
		double s = DateUtil.dateChangeSecond(DateUtil.dateChangeString(start));
		double e = DateUtil.dateChangeSecond(DateUtil.dateChangeString(end));
		return e-s;
	}
	
	/**
	 * jsonת����map
	 * @param jsonStr
	 * @return
	 * @author simon
	 * @date 2015��6��16�� ����10:06:17
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
