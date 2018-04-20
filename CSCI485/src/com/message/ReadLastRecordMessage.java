package com.message;

import java.io.Serializable;

import com.client.RID;

public class ReadLastRecordMessage implements Serializable {
	private static final long serialVerisionUID = 1;
	
	private String ChunkHandle;
	
	private RID rid;
	
	private byte[] payload; 

	public ReadLastRecordMessage(String chunkHandle, RID rid) {
		super();
		ChunkHandle = chunkHandle;
		this.rid = new RID(rid.getChunkHandle(), rid.getSlotNumber()); 
	}

	public ReadLastRecordMessage(byte[] payload, RID rid) {
		super();
		this.payload = new byte[payload.length];
		for (int i=0; i<payload.length; i++) {
			this.payload[i] = payload[i]; 
		}
		this.rid = new RID(rid.getChunkHandle(), rid.getSlotNumber()); 
	}

	public String getChunkHandle() {
		return ChunkHandle;
	}

	public RID getRid() {
		return rid;
	}

	public byte[] getPayload() {
		return payload;
	}
	
}
