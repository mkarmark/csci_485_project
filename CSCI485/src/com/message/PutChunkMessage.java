package com.message;

import java.io.Serializable;

public class PutChunkMessage implements Serializable {
	private static final long serialVerisionUID = 1;
	
	private String ChunkHandle;
	
	private int offset;
	
	private byte[] payload;
	
	private boolean status;

	public PutChunkMessage(String chunkHandle, byte[] payload, int offset) {
		super();
		ChunkHandle = chunkHandle;
		this.payload = new byte[payload.length];
		for (int i=0; i<payload.length; i++) {
			this.payload[i] = payload[i]; 
		}
		this.offset = offset;
	}

	public PutChunkMessage(boolean status) {
		super();
		//this.payload = payload;
		this.status = status; 
	}

	public String getChunkHandle() {
		return ChunkHandle;
	}

	public int getOffset() {
		return offset;
	}

	public boolean getStatus() {
		return status;
	}

	public byte[] getPayload() {
		return payload;
	}
}
