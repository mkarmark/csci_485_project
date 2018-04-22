package com.message;

import java.io.Serializable;

public class InformInitializeChunkMessage implements Serializable {
private static final long serialVerisionUID = 1;
	
	private String ChunkHandle; 
	
	public InformInitializeChunkMessage() {
		
	}
	
	public InformInitializeChunkMessage(String ChunkHandle) {
		this.ChunkHandle = ChunkHandle;
	}
	
	public String getChunkHandle() {
		return this.ChunkHandle; 
	}
}
