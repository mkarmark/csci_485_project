package com.chunkserver;

import java.io.Serializable;
import java.net.InetAddress;

public class Location implements Serializable {
	private static final long serialVerisionUID = 1;
	
	private InetAddress ip;
	private int socket;
	
	public Location(InetAddress ip, int socket) {
		super();
		this.ip = ip;
		this.socket = socket;
	}
	public InetAddress getIp() {
		return ip;
	}
	public int getSocket() {
		return socket;
	} 
	
	public String toString() {
		return ip.toString() + ": " + socket;
	}
}
