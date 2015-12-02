package util;

import clojure.main;

public class TimeUtil {
	/**
	 * 去掉时间毫秒值
	 * @param time
	 * @return
	 */
	public String formatTime(String time){
		String str = null;
		str = time.substring(0, time.length()-4);
		return str;
	}
	public static void main(String[] args) {
		TimeUtil tu = new TimeUtil();
		tu.formatTime("2015-11-12 11:12:13.999");
	}
}
