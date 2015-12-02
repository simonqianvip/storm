package test;

import java.util.HashMap;

public class App1 {

	public static void main(String[] args) throws Exception {
//		String line = "";
//		@SuppressWarnings("unchecked")
//		Map<String, String> map = (Map<String, String>) JSONValue
//				.parse(line);
		HashMap<String, String> hashMap = new HashMap<String,String>();
		String name = "jack";
		
		String str = String.valueOf(hashMap.get("name"));
		
		if(name.equals(str)){
			System.out.println("true");
		}else{
			System.out.println("false");
		}
	}
}
