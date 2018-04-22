package com.message;

import java.io.Serializable;
import java.util.Vector;

import com.client.RID;

public class InformAppendRecordMessage implements Serializable {
	private static final long serialVerisionUID = 1;
	
	private String ChunkHandle;
	
	private byte[] payload; 
	
	private String previousChunkHandle;

	public InformAppendRecordMessage(String chunkHandle, byte[] payload, String previousChunkHandle) {
		super();
		ChunkHandle = chunkHandle;
//		this.payload = payload;
		this.payload = new byte[payload.length];
		for (int i=0; i<payload.length; i++) {
			this.payload[i] = payload[i]; 
		}
		this.previousChunkHandle = previousChunkHandle;
	}

	public String getChunkHandle() {
		return ChunkHandle;
	}

	public byte[] getPayload() {
		return payload;
	}

	public String getPreviousChunkHandle() {
		return previousChunkHandle;
	}
}
