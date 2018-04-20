package com.message;

import java.io.Serializable;

public class AppendChunkToFileSpaceMessage implements Serializable {
	private static final long serialVerisionUID = 1;
	
	private String filePath;
	private String ChunkHandle;
	private boolean status;
	
	public AppendChunkToFileSpaceMessage(String filePath, String ChunkHandle) {
		this.filePath = filePath;
		this.ChunkHandle = ChunkHandle;
	}
	
	public AppendChunkToFileSpaceMessage(boolean status) {
		this.status = status;
	}
	
	public String GetFilePath() {
		return this.filePath;
	}
	
	public String GetChunkHandle() {
		return this.ChunkHandle; 
	}
	
	public boolean IsStatus() {
		return this.status; 
	}
	
	public String toString() {
		return filePath + " - " + ChunkHandle; 
	}
}
