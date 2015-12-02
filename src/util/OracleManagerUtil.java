package util;

import java.io.Serializable;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OracleManagerUtil{
	private static final long serialVersionUID = 3819302403032392506L;
	private static final String URL = "jdbc:Oracle:thin:@172.16.64.38:1521:ivrrep2";
	private static final String USER = "settle";
	private static final String PASSWORD = "jsnjivrsettle";

	public static Connection getConnection() throws SQLException, Exception {
		Connection con = null;
		try {
			Class.forName("oracle.jdbc.OracleDriver").newInstance();
			System.out.println("url=="+URL);
			System.out.println("user=="+USER);
			System.out.println("password=="+PASSWORD);
			System.out.println("连接数据库");
			con= DriverManager.getConnection(URL,USER,PASSWORD);
			if(con != null){
				System.out.println("数据库连接成功！");
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("连接数据库失败："+e.getMessage());  
		}
		return con;
	}

	public static void closeConnection(Connection con) {
		if (con != null) {
			try {
				con.close();
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	/**
	 * 调用存储过程
	 * 
	 * @param caller
	 * @param called
	 * @param chargetime
	 * @param endtime
	 * @param locationum
	 * @throws Exception
	 */
	public static void getPrepareCall(Connection con,String caller, String called,
			String chargetime, String endtime, String locationum)
			throws Exception {
		CallableStatement prepareCall = null;
		try {
			prepareCall = con
					.prepareCall("{call SETTLE.ONLINE_BILLING.BILL_MAIN(?,?,?,?,?)}");
			prepareCall.setString(1, caller);
			prepareCall.setString(2, called);
			prepareCall.setString(3, chargetime);
			prepareCall.setString(4, endtime);
			prepareCall.setString(5, locationum);
			int i = prepareCall.executeUpdate();
			if (i != 0) {
				System.out.println("insert into table is success");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			prepareCall.close();
		}
	}

	public static void main(String[] args) throws Exception {
		Connection connection = OracleManagerUtil.getConnection();
		if(connection != null){
			System.out.println("连接数据库成功！");
		}else{
			System.out.println("连接数据库失败！");
		}
	}
	
}


