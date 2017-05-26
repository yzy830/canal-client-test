package com.gerald.client.test.slave;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


public class MySqlUtils {
	
	private static Properties pros = new Properties();
	private static String url = null;
	private static String username = null;
	private static String password = null;
	
	static {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			pros.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("cookbook.properties"));
			url = pros.getProperty("url");
			System.out.println(url);
			username = pros.getProperty("username");
			password = pros.getProperty("password");
		} catch(Exception e) {
			e.printStackTrace();
			if(e instanceof SQLException) {
				printSqlException((SQLException)e);
			}
		}
	}
	
	public static Connection getConnection() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {		
		return DriverManager.getConnection(url, username, password);
	}
	
	public static void printSqlException(SQLException sqlException) {
		System.err.println("SQLException: " + sqlException.getMessage());
		System.err.println("SQLState: " + sqlException.getSQLState());
		System.err.println("Vendor code" + sqlException.getErrorCode());
	}
}
