package com.custom.build.serveralogs.connections;

import java.sql.Connection;
import java.sql.DriverManager;


import org.hsqldb.persist.HsqlProperties;

public class DBConnectionManager {
	final String dbLocation = "c:\\temp\\"; // change it to your db location
	org.hsqldb.server.Server hsqlDBServer;
	Connection dbConn = null;

	public void startDBServer() {
	    HsqlProperties props = new HsqlProperties();
	    props.setProperty("server.database.0", "file:" + dbLocation + "mydb;");
	    props.setProperty("server.dbname.0", "xdb");
	    props.setProperty("DATABASE.TEXT.TABLE.DEFAULTS", "'");
	    
	    hsqlDBServer = new org.hsqldb.Server();
	    try {
	        hsqlDBServer.setProperties(props);
	    } catch (Exception e) {
	    	System.out.println("Exceptions are -->" + e.getMessage());
	        return;
	    }
	    hsqlDBServer.start();
	}

	public void stopDBServer() {
	    hsqlDBServer.shutdown();
	}

	public Connection getDBConn() {
	    try {
	        Class.forName("org.hsqldb.jdbcDriver");
	        dbConn = DriverManager.getConnection(
	                "jdbc:hsqldb:hsql://localhost/xdb", "SA", "");
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	    return dbConn;
	}
	
}

