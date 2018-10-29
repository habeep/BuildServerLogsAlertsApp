package com.custom.build.serveralogs.main;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.custom.build.serveralogs.connections.DBConnectionManager;
import com.custom.build.serveralogs.model.Logs;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PopulateLogsAlertTest  {
	final String dbLocation = "c:\\temp\\"; // change it to your db location
	org.hsqldb.server.Server hSqlDbServer;
	final static Logger logger = Logger.getLogger(PopulateLogsAlertTest.class);
	DBConnectionManager dbm = new DBConnectionManager();
	private String jsonFileLocation = "c:\\events.json";
	
	/**
	 * Read the JSON string from the provided fileLocation, Parse and write into HSQLDB
	 * @param fileLocation
	 */

	@Test
	public void popualateintoDB() {
		logger.info("Trying to start the HSQLDB server");
		dbm.startDBServer();
		logger.info("Trying to obtain the db connection");
		Connection conn = dbm.getDBConn();
		ObjectMapper mapper = new ObjectMapper();
		try {
			Statement stmt = conn.createStatement();
			logger.info("Execute the query to create tblEvents table.");
			stmt.executeQuery("create table if not exists tblEvents (id varchar(45), state varchar(45),eventDuration int,type varchar(45),host varchar(45),alert BOOLEAN NULL)");
			logger.info("Read the JSON file and assign it into Logs list.");
			List<Logs> logs = mapper.readValue(new File(jsonFileLocation),  new TypeReference<List<Logs>>(){});
			logger.info("Process the logs list and populate it into DB which is processing time longer than 4 secs");
			logs.stream()
			.collect(Collectors.groupingBy(Logs::getId))
			.forEach((id, logWithSameId) -> {
				if (logWithSameId.size() > 1) {
					long timeDiff = 0;
					long startedTime = 0, finishedTime = 0;
					for(Logs log:logWithSameId) {
						startedTime = log.getState().equalsIgnoreCase("STARTED")?(log.getTimestamp()):startedTime;
						finishedTime = log.getState().equalsIgnoreCase("FINISHED")?(log.getTimestamp()):finishedTime;
						if(startedTime != 0 && finishedTime != 0) {
							timeDiff = finishedTime - startedTime;
							boolean alertFlag = timeDiff >= 4?true:false;

							String query = "INSERT INTO tblEvents (id,state,type,host,alert,eventDuration) values ("+"'"+log.getId()+"'"+","+"'" +log.getState()+"'"+"," +"'"+log.getType()+"'"+"," +"'"+log.getHost()+"'"+"," +"'"+alertFlag+"',"+timeDiff+")";
							try {
								if(alertFlag) {
									String checkExist = "SELECT id FROM tblEvents where id ='"+log.getId()+"'";
									ResultSet rs = stmt.executeQuery(checkExist);
									if(!rs.next()) {
										boolean status = stmt.execute(query);
										assertEquals(true, status);
									} else {
										logger.error(log.getId() + " is Exist");
									}
								}
							} catch (SQLException e) {
								logger.error("Exception "+e.getMessage());
								e.printStackTrace();
							}
							finally {
								logger.error("stop the DB Server if it is exception caught.");
							}
						}						

					}

				}
			});
		} catch (Exception e) {
			logger.error("Exception "+e.getMessage());
			e.printStackTrace();
		}
		finally {
			logger.error("stop the DB Server if it is exception caught.");
			dbm.stopDBServer();
		}

	}


	
	
	
}
