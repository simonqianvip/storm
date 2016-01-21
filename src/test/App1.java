package test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class App1 {
	private static Log log = LogFactory.getLog("App1");
	public static void main(String[] args) throws Exception {
		log.info("very good");
		log.error("error");
	}
}
