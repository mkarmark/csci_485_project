package com.message;

import java.io.Serializable;
import java.util.Vector;

import com.client.RID;

public class ReadFirstRecordMessage implements Serializable {
	private static final long serialVerisionUID = 1;
	
	private String ChunkHandle;
	
	private RID rid;
	
	private byte[] payload; 

	public ReadFirstRecordMessage(String chunkHandle, RID rid) {
		super();
		ChunkHandle = chunkHandle;
		this.rid = new RID(rid.getChunkHandle(), rid.getSlotNumber()); 
	}

	public ReadFirstRecordMessage(byte[] payload, RID rid) {
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
