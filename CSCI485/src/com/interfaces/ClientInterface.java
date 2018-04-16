package com.interfaces;

/**
 * Interfaces of the TinyFS Client 
 * @author Shahram Ghandeharizadeh
 *
 */
public interface ClientInterface {
	//add constant here so that there is no need to use ChunkServer in unit tests
	public static final int ChunkSize = 1024 * 4; //4 KB chunk sizes
	
	/**
	 * Return the chunkhandle for a newly created chunk.
	 */
	public String initializeChunk();
	
	/**
	 * Write the byte array payload to the ChunkHandle at the specified offset.
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset);
	
	/**
	 * Read the specified NumberOfBytes from the target chunk starting at the specified offset.
	 * Return the retrieved number of bytes as a byte array.
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes);	
	
}
