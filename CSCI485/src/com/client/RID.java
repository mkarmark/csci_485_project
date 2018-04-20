package com.client;

import java.io.Serializable;

public class RID implements Serializable {
	private static final long serialVerisionUID = 1;
	
	private String ChunkHandle;
	private int slotNumber; 
	
	public RID() {
		this.ChunkHandle = "";
		this.slotNumber = -1;
	}
	
	public RID(String ChunkHandle, int slotNumber) {
		this.ChunkHandle = ChunkHandle;
		this.slotNumber = slotNumber;
	}

	public String getChunkHandle() {
		return ChunkHandle;
	}

	public void setChunkHandle(String chunkHandle) {
		ChunkHandle = chunkHandle;
	}

	public int getSlotNumber() {
		return slotNumber;
	}

	public void setSlotNumber(int slotNumber) {
		this.slotNumber = slotNumber;
	}
	
	public String toString() {
		return ChunkHandle + ": " + slotNumber; 
	}
	
	
}
