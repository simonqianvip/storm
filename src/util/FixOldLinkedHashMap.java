package util;

import java.util.LinkedHashMap;
import java.util.Map;

public class FixOldLinkedHashMap extends LinkedHashMap<String,Map> {

	private static final long serialVersionUID = -2168793042947262091L;
	/**
	 * map最大存储1000万条记录
	 */
	private static int MAX_ENTRIES = 10000000;

	public static int getMAX_ENTRIES() {
		return MAX_ENTRIES;
	}

	public static void setMAX_ENTRIES(int max_entries) {
		MAX_ENTRIES = max_entries;
	}

	protected boolean removeEldestEntry(Map.Entry eldest) {
		return size() > MAX_ENTRIES;
	}

}
