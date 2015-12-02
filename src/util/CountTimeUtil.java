package util;

public class CountTimeUtil {
	/**
	 * 统计程序运行时间
	 * @param callBack
	 */
	public static void countTime(CallBack callBack) {
		long beginTime = System.currentTimeMillis();
		callBack.execute();
		long endTime = System.currentTimeMillis();
		System.out.println(" use time :" + (endTime - beginTime) + " ms");
	}
}
