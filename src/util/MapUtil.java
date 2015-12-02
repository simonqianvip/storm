package util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class MapUtil {
	/**
	 * map转换成String方法
	 * @param map
	 * @return
	 */
	public static String getString(Map map){
		Iterator<Entry<String,String>> i = map.entrySet().iterator();
        if (! i.hasNext())
            return "{}";

        StringBuilder sb = new StringBuilder();
//        sb.append('{');
        for (;;) {
            Entry<String,String> e = i.next();
            String key = String.valueOf(e.getKey());
            String value = String.valueOf(e.getValue());
            sb.append('"');
            sb.append(key);
            sb.append('"');
            sb.append(':');
            sb.append('"');
            sb.append(value);
            sb.append('"');
            if (! i.hasNext())
                return sb.toString();
//                return sb.append('}').toString();
            sb.append(',').append(' ');
        }
	}

}
