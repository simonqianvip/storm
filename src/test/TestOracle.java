package test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestOracle {
	public static void main(String[] args) throws Exception {
		TestOracle to = new TestOracle();
		for(int i=0;i<5;i++){
			to.process();
		}
	}
	
	public void process() throws Exception{
		try {
			Class.forName("oracle.jdbc.OracleDriver");
			String url="jdbc:oracle:thin:@(DESCRIPTION="
					+ "(LOAD_BALANCE=on)"
					+ "(ADDRESS=(PROTOCOL=TCP) (HOST=172.16.64.47)(PORT=1521))"
					+ "(CONNECT_DATA=(SERVICE_NAME=IVRREP)) )";
			String user="settle"; 
			String password="jsnjivrsettle"; 
			Connection conn= DriverManager.getConnection(url,user,password);
			if(conn!=null){
				System.out.println("数据库连接成功！");
			}
			Statement stmtNew=conn.createStatement();
			ResultSet set = stmtNew.executeQuery("select * from usage_charge_test");
			while(set.next()){
				String string = set.getString(5);
				System.out.println(string);
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
	}

}
