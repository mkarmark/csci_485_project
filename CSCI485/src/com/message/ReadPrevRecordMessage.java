package com.message;

import java.io.Serializable;

import com.client.RID;

public class ReadPrevRecordMessage implements Serializable {
	private static final long serialVerisionUID = 1;
	
	private RID rid;
	
	private RID prevRid; 
	
	private byte[] payload; 

	public ReadPrevRecordMessage(RID rid, RID prevRid) {
		super();
		this.rid = new RID(rid.getChunkHandle(), rid.getSlotNumber()); 
		this.prevRid = new RID(prevRid.getChunkHandle(), prevRid.getSlotNumber());
	}

	public ReadPrevRecordMessage(byte[] payload, RID rid, RID prevRid) {
		super();
		this.payload = new byte[payload.length];
		for (int i=0; i<payload.length; i++) {
			this.payload[i] = payload[i]; 
		}
		this.rid = new RID(rid.getChunkHandle(), rid.getSlotNumber()); 
		this.prevRid = new RID(prevRid.getChunkHandle(), prevRid.getSlotNumber()); 
	}

	public RID getRid() {
		return rid;
	}
	
	public RID getPrevRid() {
		return prevRid; 
	}

	public byte[] getPayload() {
		return payload;
	}
}
