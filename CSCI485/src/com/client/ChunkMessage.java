package com.client;

import java.io.Serializable;

public class ChunkMessage implements Serializable {
	public static final long serialVersionUID = 1;
	private int size;
	private int type;
	private int offset;
	private String chunkHandle;
	private byte[] chunk;
	
	public ChunkMessage(int size, int type)
	{
		this.size = size;
		this.type = type;
	}
	
	public void setOffset(int offset)
	{
		this.offset = offset;
	}
	
	public void setChunk(byte[] chunk)
	{
		this.chunk = chunk;
	}
	
	public void setHandle(String chunkHandle)
	{
		this.chunkHandle = chunkHandle;
	}
	
	public int getSize() {
		return size;
	}
	
	public int getType() {
		return type;
	}
	
	public int getOffset() {
		return offset;
	}
	
	public String getHandle() {
		return chunkHandle;
	}
	
	public byte[] getChunk() {
		return chunk;
	}
}
