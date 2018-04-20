package com.message;

import java.io.Serializable;
import java.util.Vector;

import com.client.RID;

public class AppendRecordMessage implements Serializable {
	private static final long serialVerisionUID = 1;
	
	private String ChunkHandle;
	
	private byte[] payload; 
	
	private String previousChunkHandle;
	
	private Vector<RID> rids;

	public AppendRecordMessage(String chunkHandle, byte[] payload, String previousChunkHandle) {
		super();
		ChunkHandle = chunkHandle;
//		this.payload = payload;
		this.payload = new byte[payload.length];
		for (int i=0; i<payload.length; i++) {
			this.payload[i] = payload[i]; 
		}
		this.previousChunkHandle = previousChunkHandle;
	}

	public AppendRecordMessage(Vector<RID> rids) {
		super();
		this.rids = new Vector<RID>();
		for (int i=0; i<rids.size(); i++) {
			RID rid = new RID(rids.get(i).getChunkHandle(), rids.get(i).getSlotNumber());
			this.rids.addElement(rid);
		}
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

	public Vector<RID> getRids() {
		return rids;
	} 
	
}
