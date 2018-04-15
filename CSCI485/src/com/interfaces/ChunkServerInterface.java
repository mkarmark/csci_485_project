package com.interfaces;

import java.util.Vector;

import com.client.RID;

/**
 * Interfaces of the CSCI 485 TinyFS ChunkServer
 * @author Shahram Ghandeharizaden
 *
 */
public interface ChunkServerInterface {

	public static final int ChunkSize = 1024 * 1024; //4 KB chunk sizes
	
	/**
	 * Return the chunkhandle for a newly created chunk.
	 */
	public String initializeChunk();
	
	/**
	 * Write the byte array plod to the ChunkHandle at the specified offset.
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset);
	
	/**
	 * Read the specified NumberOfBytes from the target chunk starting at the specified offset.
	 * Return the retrieved number of bytes as a byte array.
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes);	
	
	/**
	 * Append payload to the end of the file 
	 * Create an extra chunk if the last chunk does not have space
	 * Returns RID the payload is on over which the byte payload was appended
	 */
	public RID appendRecord(String ChunkHandle, byte[] payload);
	
	/**
	 * Delete payload at slot number indicated in RID from ChunkHandle
	 * returns true if success false otherwise
	 */
	public boolean deleteRecord(RID rid);
	
	/**
	 * Return byte[] payload corresponding to first record
	 */
	public byte[] readFirstRecord(String ChunkHandle, RID rid);
	
	/**
	 * Return byte[] payload corresponding to last record
	 */
	public byte[] readLastRecord(String ChunkHandle, RID rid);
	
	/**
	 * Return byte[] payload that is the record after the one corresponding to the RID
	 */
	public byte[] readNextRecord(RID rid, RID nextRid);
	
	/**
	 * Return byte[] payload that is the record before the one corresponding to the RID
	 */
	public byte[] readPrevRecord(RID rid, RID prevRid);
}
