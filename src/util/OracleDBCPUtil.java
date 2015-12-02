package util;

import java.io.InputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;


public class OracleDBCPUtil implements Serializable{
	private static DataSource ds;
	
	static {
		try {
			InputStream inStream = ClassLoader.getSystemClassLoader().getResourceAsStream("OracleDBCPConfig.properties");
//			System.out.println("实例名=="+OracleDBCPUtil.class.getClassLoader().getResourceAsStream("OracleDBCPConfig.properties"));
			Properties props = new Properties();
			props.load(inStream);
			ds = BasicDataSourceFactory.createDataSource(props);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("***************************************************DataSource初始化错误！***************************************************");  

		}
	}

	public static DataSource getDataSource() {
		return ds;
	}

	public static Connection getConnection(){
		try {
			return ds.getConnection();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	//回收数据库连接  
	   protected static void shutdownDataSource() throws SQLException {  
	       BasicDataSource bds = (BasicDataSource) ds;  
	       bds.close();  
	   } 


}
