package com.custom.build.serveralogs.model;

public class Logs {

	private String id;
	private String state;
	private String type;
	private String host;
	private long timestamp;
	private long timeDiff;
	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}
	/**
	 * @return the state
	 */
	public String getState() {
		return state;
	}
	/**
	 * @param state the state to set
	 */
	public void setState(String state) {
		this.state = state;
	}
	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}
	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}
	/**
	 * @return the host
	 */
	public String getHost() {
		return host;
	}
	/**
	 * @param host the host to set
	 */
	public void setHost(String host) {
		this.host = host;
	}
	/**
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}
	/**
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	
	/**
	 * @return the timeDiff
	 */
	public long getTimeDiff() {
		return timeDiff;
	}
	/**
	 * @param timeDiff the timeDiff to set
	 */
	public void setTimeDiff(long timeDiff) {
		this.timeDiff = timeDiff;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Events [id=" + id + ", state=" + state + ", type=" + type + ", host=" + host + ", timestamp="
				+ timestamp + ", timeDiff=" + timeDiff + "]";
	}
	
	
	
}
