package com.blueteak.fbleads.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class FbLeadsJdbcTest {
	static final String DB_URL = "jdbc:sqlserver://15.0.4.4:1433;databaseName=dms-leads";
	static final String USER = "sa";
	static final String PASS = "h32YP9cw4N2VJrX8sd";
	public static String fbSelectQuery = "select * from T_LEAD "
			+ "where "
			//+ "CREATED_DATE_TIME >= '%1$s' and "
			+ "CUST_NAME =  '%1$s' "
			+ "and EMAIL  =  '%2$s' and CONVERT(VARCHAR(MAX), NOTES) = '%3$s' and PHONE = '%4$s' and  src='%5$s' ";

	 public static void main(String[] args) {
		    Connection connection = null;
		    Statement selectStmt = null;
		    try
		    {
		      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		      connection = DriverManager.getConnection(DB_URL, USER, PASS);
		      String selectQuery = String.format(fbSelectQuery, 
		    		  "Stephen Roy",
		    		  "Cnu2918@gmail.com",
		    		  "HILUX",
		    		  "+60-0169488484",
		    		  "FB");
		      System.out.println("select Query :: "+selectQuery);
		      selectStmt = connection.createStatement();
		      ResultSet rs = selectStmt.executeQuery(selectQuery);
		      while(rs.next())
		      {
		        System.out.println(rs.getString(1));  //First Column
		        System.out.println(rs.getString(2));  //Second Column
		        System.out.println(rs.getString(3));  //Third Column
		        System.out.println(rs.getString(4));  //Fourth Column
		      }
		    } 
		    catch (Exception e) {
		      e.printStackTrace();
		    }finally {
		      try {
		        selectStmt.close();
		        connection.close();
		      } catch (Exception e) {
		        e.printStackTrace();
		      }
		    }
		  }
	 

}
