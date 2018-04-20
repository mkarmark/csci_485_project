package com.message;

import java.io.Serializable;

public class InitializeChunkMessage implements Serializable{
	private static final long serialVerisionUID = 1;
	
	private String ChunkHandle; 
	
	public InitializeChunkMessage() {
		
	}
	
	public InitializeChunkMessage(String ChunkHandle) {
		this.ChunkHandle = ChunkHandle;
	}
	
	public String getChunkHandle() {
		return this.ChunkHandle; 
	}
}
