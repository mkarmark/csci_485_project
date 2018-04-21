package com.message;

import java.io.Serializable;

import com.chunkserver.Location;

public class ChunkServerIdentityMessage implements Serializable {
	private static final long serialVerisionUID = 1;
	
	private Location location;
	private int chunkServerID;
	
	public ChunkServerIdentityMessage(Location location) {
		super();
		this.location = location;
	}

	public ChunkServerIdentityMessage(int chunkServerID) {
		super();
		this.chunkServerID = chunkServerID;
	}

	public Location getLocation() {
		return location;
	}

	public int getChunkServerID() {
		return chunkServerID;
	}
	
}
