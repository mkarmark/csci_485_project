package com.message;

import java.io.Serializable;

import com.client.RID;

public class ReadNextRecordMessage implements Serializable {
	private static final long serialVerisionUID = 1;
	
	private RID rid;
	
	private RID nextRid; 
	
	private byte[] payload; 

	public ReadNextRecordMessage(RID rid, RID nextRid) {
		super();
		this.rid = new RID(rid.getChunkHandle(), rid.getSlotNumber()); 
		this.nextRid = new RID(nextRid.getChunkHandle(), nextRid.getSlotNumber());
	}

	public ReadNextRecordMessage(byte[] payload, RID rid, RID nextRid) {
		super();
		this.payload = new byte[payload.length];
		for (int i=0; i<payload.length; i++) {
			this.payload[i] = payload[i]; 
		}
		this.rid = new RID(rid.getChunkHandle(), rid.getSlotNumber()); 
		this.nextRid = new RID(nextRid.getChunkHandle(), nextRid.getSlotNumber()); 
	}

	public RID getRid() {
		return rid;
	}
	
	public RID getNextRid() {
		return nextRid; 
	}

	public byte[] getPayload() {
		return payload;
	}
}
