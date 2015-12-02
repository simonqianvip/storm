package util;

public class UuidUtil {
	/**
	 * 获取时间uuid中的日期
	 * @param uuid
	 */
	public static String parseUUID(String uuid){
		
		if(uuid.length() != 0 && uuid != null){
			String[] splits = uuid.split("_");
			String str = splits[splits.length-1];
			String month = str.substring(0, 6);
			return month;
		}
		return uuid;
	}
	public static void main(String[] args) {
		System.out.println(UuidUtil.parseUUID("02008e4e_20151105161606994"));;
	}
}
