package com.message;

import java.io.Serializable;

public class GetChunkMessage implements Serializable {
	private static final long serialVerisionUID = 1;
	
	private String ChunkHandle;
	
	private int offset;
	
	private int NumberOfBytes;
	
	private byte[] payload;

	public GetChunkMessage(String chunkHandle, int offset, int numberOfBytes) {
		super();
		ChunkHandle = chunkHandle;
		this.offset = offset;
		NumberOfBytes = numberOfBytes;
	}

	public GetChunkMessage(byte[] payload) {
		super();
		//this.payload = payload;
		this.payload = new byte[payload.length];
		for (int i=0; i<payload.length; i++) {
			this.payload[i] = payload[i]; 
		}
	}

	public String getChunkHandle() {
		return ChunkHandle;
	}

	public int getOffset() {
		return offset;
	}

	public int getNumberOfBytes() {
		return NumberOfBytes;
	}

	public byte[] getPayload() {
		return payload;
	}
}
