package test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class App2 {
	public static void main(String[] args) throws Exception {
		String str = "[2015-07-09 18:05:07] [0x51077A2F]      SAssign( SR55[\"\"] _Field15[\"\"] )";
		if(str.contains("1077A2F")){
			System.out.println(str);
		}
		
		
		App2 app2 = new App2();
//		app2.sort();
	}
	
	public String sort(List arrayList){
		List<String> list = new ArrayList<String>();
		for(Object line:arrayList){
			String str = String.valueOf(line);
			String[] split = str.split(".log_");
			list.add(split[split.length-1]);
		}
		Collections.sort(list);
		
		System.out.println(list.get(list.size()-1));
		return list.get(list.size()-1);
	}

	
}
