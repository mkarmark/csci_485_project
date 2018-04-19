package com.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Scanner;
import java.util.Vector;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;
import com.message.AppendChunkToFileSpaceMessage;
import com.message.CreateDirMessage;

public class ClientRec {
	private ObjectInputStream csOis;
	private ObjectOutputStream csOos;
	private ObjectInputStream msOis;
	private ObjectOutputStream msOos; 
	
	public static ChunkServer cs = new ChunkServer();
	
	public ClientRec() {
		int port = 5959;
		
		// Get the port number
		File portFile = new File("ChunkServerPort.txt");
		Scanner scanner;
		try {
			scanner = new Scanner(portFile);
			while(scanner.hasNext())
			{
				port = scanner.nextInt();
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			System.out.println("port file not found");
		}
		
		// Connect to the port
		try {
			System.out.println("Trying to connect to ChunkServer");
			// TODO: Get IP address of master from file
			Socket s = new Socket("localhost", port);
			
			csOis = new ObjectInputStream(s.getInputStream());
			csOos = new ObjectOutputStream(s.getOutputStream());
		} catch (IOException ioe) {
			System.out.println("ioe in clientFS constructor: " + ioe.getMessage());
		}
		
		port = 5858;
		
		// Get the port number
		portFile = new File("MasterPort.txt");
		try {
			scanner = new Scanner(portFile);
			while(scanner.hasNext())
			{
				port = scanner.nextInt();
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			System.out.println("port file not found");
		}
		
		// Connect to the port
		try {
			System.out.println("Trying to connect to Master");
			// TODO: Get IP address of master from file
			Socket s = new Socket("localhost", port);
			
			msOis = new ObjectInputStream(s.getInputStream());
			msOos = new ObjectOutputStream(s.getOutputStream());
		} catch (IOException ioe) {
			System.out.println("ioe in clientFS constructor: " + ioe.getMessage());
		}		
		
	}

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
			AppendChunkToFileSpaceMessage actfsm = new AppendChunkToFileSpaceMessage(ofh.getFilepath(), ch);
			try{
				// Send the message
				msOos.writeObject(actfsm);
				msOos.flush();
				
				// Receive the response and cast
				Object o = null;
				o = msOis.readObject();
				actfsm = (AppendChunkToFileSpaceMessage)o;
				
				// Reset both streams
				msOos.reset();
			} catch (IOException ioe) {
				System.out.println("ioe in clientFS: "+ioe.getMessage());
			} catch (ClassNotFoundException cnfe) {
				System.out.println("In ClientFS createDir " + cnfe.getMessage());
			}
			
			// Get error from message
			boolean status = actfsm.IsStatus();
			if (!status) {
				System.out.println("clientRec has not properly communicated with master");
			}		
		}
		
		// Get record ID of appended record
		Vector<RID> appendedRIDs = cs.appendRecord(ofh.getLastChunk(), payload, "-1");
		
		
		RID firstRID = appendedRIDs.firstElement(); 
		if (!ofh.getLastChunk().equals(firstRID.getChunkHandle())) {
			ofh.appendChunk(firstRID.getChunkHandle()); 
			AppendChunkToFileSpaceMessage actfsm = new AppendChunkToFileSpaceMessage(ofh.getFilepath(), firstRID.getChunkHandle());
			
			try{
				// Send the message
				msOos.writeObject(actfsm);
				msOos.flush();
				
				// Receive the response and cast
				Object o = null;
				o = msOis.readObject();
				actfsm = (AppendChunkToFileSpaceMessage)o;
				
				// Reset both streams
				msOos.reset();
			} catch (IOException ioe) {
				System.out.println("ioe in clientFS: "+ioe.getMessage());
			} catch (ClassNotFoundException cnfe) {
				System.out.println("In ClientFS createDir " + cnfe.getMessage());
			}
			
			// Get error from message
			boolean status = actfsm.IsStatus();
			if (!status) {
				System.out.println("clientRec has not properly communicated with master");
			}		
		}
		for (int i=1; i<appendedRIDs.size(); i++) {
			ofh.appendChunk(appendedRIDs.get(i).getChunkHandle());
			AppendChunkToFileSpaceMessage actfsm = new AppendChunkToFileSpaceMessage(ofh.getFilepath(), appendedRIDs.get(i).getChunkHandle());
			
			try{
				// Send the message
				msOos.writeObject(actfsm);
				msOos.flush();
				
				// Receive the response and cast
				Object o = null;
				o = msOis.readObject();
				actfsm = (AppendChunkToFileSpaceMessage)o;
				
				// Reset both streams
				msOos.reset();
			} catch (IOException ioe) {
				System.out.println("ioe in clientFS: "+ioe.getMessage());
			} catch (ClassNotFoundException cnfe) {
				System.out.println("In ClientFS createDir " + cnfe.getMessage());
			}
			
			// Get error from message
			boolean status = actfsm.IsStatus();
			if (!status) {
				System.out.println("clientRec has not properly communicated with master");
			}
		}

		// Deep copy into RecordID
		RecordID.setChunkHandle(firstRID.getChunkHandle());
		RecordID.setSlotNumber(firstRID.getSlotNumber());
		
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
		boolean status = cs.deleteRecord(RecordID, ofh.getFirstChunk());
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
//		System.out.println("Currently read " + pivot + " and next one is " + rid);
		while (payload == null) {
//			System.out.println("Gotta move on to next slot");
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
