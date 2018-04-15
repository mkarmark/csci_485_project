package com.client;

import java.nio.ByteBuffer;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;

public class ClientRec {
	
	public static ChunkServer cs = new ChunkServer();

	/**
	 * Appends a record to the open file as specified by ofh Returns BadHandle
	 * if ofh is invalid Returns BadRecID if the specified RID is not null
	 * Returns RecordTooLong if the size of payload exceeds chunksize RID is
	 * null if AppendRecord fails
	 *
	 * Example usage: AppendRecord(FH1, obama, RecID1)
	 */
	public FSReturnVals AppendRecord(FileHandle ofh, byte[] payload, RID RecordID) {
		// If specified record ID is not null, return BadRecID
		if(RecordID.getSlotNumber()!=-1)
		{
			return FSReturnVals.BadRecID;
		}
		
		// TODO: Check if the filehandle is bad
		
		// Initialize first chunk of file if needed (TODO: Via networking to ChunkServer)
		if(ofh.getChunks().isEmpty())
		{
			String ch = cs.initializeChunk();
			ofh.appendChunk(ch);
		}
		
		// Get record ID of appended record
		RID appendedRID = cs.appendRecord(ofh.getLastChunk(), payload);
		
		// Deep copy into RecordID
		RecordID.setChunkHandle(appendedRID.getChunkHandle());
		RecordID.setSlotNumber(appendedRID.getSlotNumber());
		
		// If a new chunk had to be created, add that to ofh
		if(!RecordID.getChunkHandle().equals(ofh.getLastChunk()))
		{
			ofh.appendChunk(RecordID.getChunkHandle());
		}
		
		return null;
	}

	/**
	 * Deletes the specified record by RecordID from the open file specified by
	 * ofh Returns BadHandle if ofh is invalid Returns BadRecID if the specified
	 * RID is not valid Returns RecDoesNotExist if the record specified by
	 * RecordID does not exist.
	 *
	 * Example usage: DeleteRecord(FH1, RecID1)
	 */
	public FSReturnVals DeleteRecord(FileHandle ofh, RID RecordID) {
		// TODO: Check if RecordID is valid
		// TODO: Check if ofh is valid
		boolean status = cs.deleteRecord(RecordID);
		if (status) return FSReturnVals.Success;  
		return null;
	}

	/**
	 * Reads the first record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadFirstRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec){
		// TODO: Return badhandle if ofh is invalid
		
		// Read first record from chunkserver
		RID rid = new RID(); 
		byte[] payload = cs.readFirstRecord(ofh.getFirstChunk(), rid);
		rec.setPayload(payload);
		rec.setRID(rid);
		
		if(payload == null)
		{
			return FSReturnVals.RecDoesNotExist;
		}
		
		// TODO: ReadFirstRecord is supposed to set the RID of TinyRec
		// Set the TinyRec Payload
		rec.setPayload(payload);
		
		return null;
	}

	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadLastRecord(FileHandle ofh, TinyRec rec){
		// TODO: Return badhandle if ofh is invalid

		// Read last record from chunkserver
		RID rid = new RID();
		byte[] payload = cs.readLastRecord(ofh.getLastChunk(), rid);
		rec.setPayload(payload);
		rec.setRID(rid);
		
		if(payload == null)
		{
			return FSReturnVals.RecDoesNotExist;
		}
		
		// Set the TinyRec Payload
		rec.setPayload(payload);
		
		return null;
	}

	/**
	 * Reads the next record after the specified pivot of the file specified by
	 * ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadFirstRecord(FH1, tinyRec1) 2. ReadNextRecord(FH1,
	 * rec1, tinyRec2) 3. ReadNextRecord(FH1, rec2, tinyRec3)
	 */
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec rec){
		// TODO: Return badhandle if ofh is invalid

		// Read next record from chunkserver
//		System.out.println("pivot: " + pivot);
		RID rid = new RID(); 
		byte[] payload = cs.readNextRecord(pivot, rid);
		while (payload == null) {
			String ChunkHandle = pivot.getChunkHandle(); 
			String newChunkHandle = ofh.getNextChunk(ChunkHandle); 
			if (newChunkHandle == null) {
				break; 
			}
			int slotNumber = 0;
			RID newPivot = new RID(newChunkHandle, slotNumber);
			payload = cs.readNextRecord(newPivot, rid);
		}
		if(payload == null)
		{
			return FSReturnVals.RecDoesNotExist;
		}
		rec.setPayload(payload);
		rec.setRID(rid);
		
		// Set the TinyRec Payload
		rec.setPayload(payload);
		
		return null;
	}

	/**
	 * Reads the previous record after the specified pivot of the file specified
	 * by ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadLastRecord(FH1, tinyRec1) 2. ReadPrevRecord(FH1,
	 * recn-1, tinyRec2) 3. ReadPrevRecord(FH1, recn-2, tinyRec3)
	 */
	public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, TinyRec rec){
		// TODO: Return badhandle if ofh is invalid

		// Read previous record from chunkserver
		RID rid = new RID();
		byte[] payload = cs.readPrevRecord(pivot, rid);
		while (payload == null) {
			String ChunkHandle = pivot.getChunkHandle();
			String prevChunkHandle = ofh.getPrevChunk(ChunkHandle);
			if (prevChunkHandle == null) {
				break;
			}
			byte[] bytesNumSlotsInPrevFile = cs.getChunk(prevChunkHandle, 0, 4);
			int intNumSlotsInPrevFile = ByteBuffer.wrap(bytesNumSlotsInPrevFile).getInt();
			RID newPivot = new RID(prevChunkHandle, intNumSlotsInPrevFile+1);
			payload = cs.readPrevRecord(newPivot, rid);
		}
		
		if(payload == null)
		{
			return FSReturnVals.RecDoesNotExist;
		}
		
		rec.setPayload(payload);
		rec.setRID(rid);
		
		// Set the TinyRec Payload
		rec.setPayload(payload);
		
		return null;
	}

}
