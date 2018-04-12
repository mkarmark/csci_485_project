package com.chunkserver;

import java.nio.ByteBuffer;

public class Chunk {
	private byte[] indexedHeap; 
	
	public Chunk()
	{
		indexedHeap = new byte[ChunkServer.ChunkSize];
		ByteBuffer b = ByteBuffer.allocate(4); 
		b.putInt(0); 
		byte[] numSlots = b.array(); 
		for (int i=0; i<numSlots.length; i++)
		{
			indexedHeap[i] = numSlots[i]; 
		}
	}
	
	
	
}
