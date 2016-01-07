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
			String chargetime, String endtime, String locationum,String uuid)
			throws Exception {
		CallableStatement prepareCall = null;
		try {
			prepareCall = con
					.prepareCall("{call SETTLE.ONLINE_BILLING.BILL_MAIN(?,?,?,?,?,?)}");
			prepareCall.setString(1, caller);
			prepareCall.setString(2, called);
			prepareCall.setString(3, chargetime);
			prepareCall.setString(4, endtime);
			prepareCall.setString(5, locationum);
			prepareCall.setString(5, uuid);
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
	
	
	public static void getPrepareADDCall(Connection con,String I_BEGINTIME,String  I_CALLED,String  I_CALLER,String  I_CALLREF,
			String  I_CALLTAG,String  I_EHANGIP,String  I_HUAWEIIP, 
			String I_ID,String  I_LOCATIONUM,String  I_RBUSNO,String  I_SIPHANDLE,
			String  I_TELHANDLE,String  I_TOSTATION,String  I_UUID)
			throws Exception {
		CallableStatement prepareCall = null;
		try {
			prepareCall = con
					.prepareCall("{call SETTLE.ONLINE_BILLING.BILL_MAIN(?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
			prepareCall.setString(1, I_BEGINTIME);
			prepareCall.setString(2, I_CALLED);
			prepareCall.setString(3, I_CALLER);
			prepareCall.setString(4, I_CALLREF);
			prepareCall.setString(5, I_CALLTAG);
			prepareCall.setString(6, I_EHANGIP);
			prepareCall.setString(7, I_HUAWEIIP);
			prepareCall.setString(8, I_ID);
			prepareCall.setString(9, I_LOCATIONUM);
			prepareCall.setString(10, I_RBUSNO);
			prepareCall.setString(11, I_SIPHANDLE);
			prepareCall.setString(12, I_TELHANDLE);
			prepareCall.setString(13, I_TOSTATION);
			prepareCall.setString(14, I_UUID);
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

//	public static void main(String[] args) throws Exception {
//		Connection connection = OracleManagerUtil.getConnection();
//		if(connection != null){
//			System.out.println("连接数据库成功！");
//		}else{
//			System.out.println("连接数据库失败！");
//		}
//	}
	
}


