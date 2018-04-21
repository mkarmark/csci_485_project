package com.message;

import java.io.Serializable;

public class InformMasterOfChunkMessage implements Serializable {
	private static final long serialVerisionUID = 1;
	
	public int chunkServerID;
	
	public String ChunkHandle;

	public InformMasterOfChunkMessage(int chunkServerID, String chunkHandle) {
		super();
		this.chunkServerID = chunkServerID;
		ChunkHandle = chunkHandle;
	}

	public int getChunkServerID() {
		return chunkServerID;
	}

	public String getChunkHandle() {
		return ChunkHandle;
	}
	
}
