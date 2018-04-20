package com.message;

import java.io.Serializable;

import com.client.RID;

public class DeleteRecordMessage implements Serializable {
	private static final long serialVerisionUID = 1;
	
	private RID rid;
	private String firstChunkHandle;
	private boolean isDeleted;
	
	public DeleteRecordMessage(RID rid, String firstChunkHandle) {
		super();
		this.rid = new RID();
		this.rid.setChunkHandle(rid.getChunkHandle());
		this.rid.setSlotNumber(rid.getSlotNumber());
		this.firstChunkHandle = firstChunkHandle;
	}
	
	public DeleteRecordMessage(boolean isDeleted) {
		super();
		this.isDeleted = isDeleted;
	}

	public RID getRid() {
		return rid;
	}

	public String getFirstChunkHandle() {
		return firstChunkHandle;
	}

	public boolean isDeleted() {
		return isDeleted;
	}

}
