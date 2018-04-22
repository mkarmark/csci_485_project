package com.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Scanner;
import java.util.Vector;

import com.chunkserver.ChunkServer;
import com.chunkserver.Location;
import com.client.ClientFS.FSReturnVals;
import com.message.AppendChunkToFileSpaceMessage;
import com.message.AppendRecordMessage;
import com.message.CreateDirMessage;
import com.message.DeleteRecordMessage;
import com.message.GetChunkMessage;
import com.message.InformAppendRecordMessage;
import com.message.InitializeChunkMessage;
import com.message.ReadFirstRecordMessage;
import com.message.ReadLastRecordMessage;
import com.message.ReadNextRecordMessage;
import com.message.ReadPrevRecordMessage;
import com.message.RequestChunkServerLocationsMessage;

public class ClientRec {
	private ObjectInputStream csOis;
	private ObjectOutputStream csOos;
	private ObjectInputStream msOis;
	private ObjectOutputStream msOos; 
	
	public static ChunkServer cs = new ChunkServer();
	
	private Vector<Location> locations; 
	
	public ClientRec() {
		int port = 5858; 
		String ipAddress = "";
		Scanner scanner = new Scanner(System.in);
		File portFile = new File("MasterPort.txt");
		try {
			scanner = new Scanner(portFile);
			
			ipAddress = scanner.nextLine();
			port = scanner.nextInt();
			System.out.println("ipAddress: " + ipAddress + "  port: " + port);
			
			scanner.close();
		} catch (FileNotFoundException e) {
			System.out.println("port file not found");
		}
		
		// Connect to the port
		InetAddress sIP = null;
				
		try {
			System.out.println("Trying to connect to Master");
			Socket s = new Socket(ipAddress, port);

			msOis = new ObjectInputStream(s.getInputStream());
			msOos = new ObjectOutputStream(s.getOutputStream());
		} catch (IOException ioe) {
			System.out.println("ioe in clientFS constructor: " + ioe.getMessage());
		}
		
		RequestChunkServerLocationsMessage rcslm = new RequestChunkServerLocationsMessage();
		
		try{
			// Send the message
			msOos.writeObject(rcslm);
			msOos.flush();
			
			// Receive the response and cast
			Object o = null;
			o = msOis.readObject();
			rcslm = (RequestChunkServerLocationsMessage)o;
			
			// Reset both streams
			msOos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in clientFS: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In ClientFS createDir " + cnfe.getMessage());
		}
		
		// Get error from message
		locations = rcslm.getLocations();  
		
//		port = 5959;
//		
//		// Get the port number
//		File portFile = new File("ChunkServerPort.txt");
//		
//		try {
//			scanner = new Scanner(portFile);
//			while(scanner.hasNext())
//			{
//				port = scanner.nextInt();
//			}
//			scanner.close();
//		} catch (FileNotFoundException e) {
//			System.out.println("port file not found");
//		}
		
		// Connect to the port
		try {
			System.out.println("Trying to connect to ChunkServer");
			// TODO: Get IP address of master from file
			Socket s = new Socket(locations.get(0).getIp(), locations.get(0).getSocket());
			
			csOis = new ObjectInputStream(s.getInputStream());
			csOos = new ObjectOutputStream(s.getOutputStream());
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
//			String ch = cs.initializeChunk();
			InitializeChunkMessage icm = new InitializeChunkMessage(); 
			try {
				// Send the message
				csOos.writeObject(icm);
				csOos.flush();
				
				// Receive the response and cast
				Object o = null;
				o = csOis.readObject();
				icm = (InitializeChunkMessage)o;
				
				// Reset both streams
				csOos.reset();
			} catch (IOException ioe) {
				System.out.println("ioe in clientFS: "+ioe.getMessage());
			} catch (ClassNotFoundException cnfe) {
				System.out.println("In ClientFS createDir " + cnfe.getMessage());
			}
			String ch = icm.getChunkHandle(); 
			
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
		Vector<RID> appendedRIDs = CommunicateAppendToCS(ofh.getLastChunk(), payload, "-1");
		
		
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
		boolean status = CommunicateDeleteToCS(RecordID, ofh.getFirstChunk());
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
		byte[] payload = CommunicateReadFirstToCS(ofh.getFirstChunk(), rid);
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
		byte[] payload = CommunicateReadLastToCS(ofh.getLastChunk(), rid);
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
		byte[] payload = CommunicateReadNextToCS(pivot, rid);
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
			payload = CommunicateReadNextToCS(newPivot, rid);
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
		byte[] payload = CommunicateReadPrevToCS(pivot, rid);
		while (payload == null) {
			String ChunkHandle = pivot.getChunkHandle();
			String prevChunkHandle = ofh.getPrevChunk(ChunkHandle);
			if (prevChunkHandle == null) {
				break;
			}			
			
			//byte[] bytesNumSlotsInPrevFile = cs.getChunk(prevChunkHandle, 0, 4);
			
			GetChunkMessage gcm = new GetChunkMessage(prevChunkHandle, 0, 4);
			try {
				// Send the message
				csOos.writeObject(gcm);
				csOos.flush();
				
				// Receive the response and cast
				Object o = null;
				o = csOis.readObject();
				gcm = (GetChunkMessage)o;
				
				// Reset both streams
				csOos.reset();
			} catch (IOException ioe) {
				System.out.println("ioe in clientRec: "+ioe.getMessage());
			} catch (ClassNotFoundException cnfe) {
				System.out.println("In ClientRec ReadPrevRecord " + cnfe.getMessage());
			}
			byte[] bytesNumSlotsInPrevFile = gcm.getPayload(); 
			int intNumSlotsInPrevFile = ByteBuffer.wrap(bytesNumSlotsInPrevFile).getInt();
			RID newPivot = new RID(prevChunkHandle, intNumSlotsInPrevFile+1);
			payload = CommunicateReadPrevToCS(newPivot, rid); 
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

	private Vector<RID> CommunicateAppendToCS(String chunkHandle, byte[] payload, String previousChunkHandle) {
		AppendRecordMessage arm = new AppendRecordMessage(chunkHandle, payload, previousChunkHandle);
		try {
			// Send the message
			csOos.writeObject(arm);
			csOos.flush();
			
			// Receive the response and cast
			Object o = null;
			o = csOis.readObject();
			arm = (AppendRecordMessage)o;
			
			// Reset both streams
			csOos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in clientRec: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In ClientRec CommunicateAppendToCS " + cnfe.getMessage());
		}
		
		for (int i=1; i<locations.size(); i++) {
			ObjectInputStream ois = null;
			ObjectOutputStream oos = null;
			try {
				System.out.println("Trying to connect to ChunkServer");
				// TODO: Get IP address of master from file
				Socket s = new Socket(locations.get(i).getIp(), locations.get(i).getSocket());
				
				ois = new ObjectInputStream(s.getInputStream());
				oos = new ObjectOutputStream(s.getOutputStream());
			} catch (IOException ioe) {
				System.out.println("ioe in clientFS constructor: " + ioe.getMessage());
			}
			InformAppendRecordMessage iarm = new InformAppendRecordMessage(chunkHandle, payload, previousChunkHandle);
			try {
				// Send the message
				oos.writeObject(arm);
				oos.flush();
				
				// Reset both streams
				oos.reset();
			} catch (IOException ioe) {
				System.out.println("ioe in clientRec: "+ioe.getMessage());
			} 
		}
		return arm.getRids(); 
	}
	
	private boolean CommunicateDeleteToCS(RID rid, String firstChunkHandle) {
		DeleteRecordMessage drm = new DeleteRecordMessage(rid, firstChunkHandle);
		try {
			// Send the message
			csOos.writeObject(drm);
			csOos.flush();
			
			// Receive the response and cast
			Object o = null;
			o = csOis.readObject();
			drm = (DeleteRecordMessage)o;
			
			// Reset both streams
			csOos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in clientRec: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In ClientRec CommunicateAppendToCS " + cnfe.getMessage());
		}
		return drm.isDeleted(); 
	}
	
	private byte[] CommunicateReadFirstToCS(String ChunkHandle, RID rid) {
		ReadFirstRecordMessage rfrm = new ReadFirstRecordMessage(ChunkHandle, rid);
		try {
			// Send the message
			csOos.writeObject(rfrm);
			csOos.flush();
			
			// Receive the response and cast
			Object o = null;
			o = csOis.readObject();
			rfrm = (ReadFirstRecordMessage)o;
			
			// Reset both streams
			csOos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in clientRec: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In ClientRec CommunicateAppendToCS " + cnfe.getMessage());
		}
		rid.setChunkHandle(rfrm.getRid().getChunkHandle());
		rid.setSlotNumber(rfrm.getRid().getSlotNumber());
		return rfrm.getPayload();  
	}
	
	private byte[] CommunicateReadLastToCS(String ChunkHandle, RID rid) {
		ReadLastRecordMessage rlrm = new ReadLastRecordMessage(ChunkHandle, rid);
		try {
			// Send the message
			csOos.writeObject(rlrm);
			csOos.flush();
			
			// Receive the response and cast
			Object o = null;
			o = csOis.readObject();
			rlrm = (ReadLastRecordMessage)o;
			
			// Reset both streams
			csOos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in clientRec: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In ClientRec CommunicateAppendToCS " + cnfe.getMessage());
		}
		rid.setChunkHandle(rlrm.getRid().getChunkHandle());
		rid.setSlotNumber(rlrm.getRid().getSlotNumber());
		return rlrm.getPayload();  
	}
	
	private byte[] CommunicateReadNextToCS(RID rid, RID nextRid) {
		ReadNextRecordMessage rnrm = new ReadNextRecordMessage(rid, nextRid);
		try {
			// Send the message
			csOos.writeObject(rnrm);
			csOos.flush();
			
			// Receive the response and cast
			Object o = null;
			o = csOis.readObject();
			rnrm = (ReadNextRecordMessage)o;
			
			// Reset both streams
			csOos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in clientRec: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In ClientRec CommunicateAppendToCS " + cnfe.getMessage());
		}
		rid.setChunkHandle(rnrm.getRid().getChunkHandle());
		rid.setSlotNumber(rnrm.getRid().getSlotNumber());
		nextRid.setChunkHandle(rnrm.getNextRid().getChunkHandle());
		nextRid.setSlotNumber(rnrm.getNextRid().getSlotNumber());
		return rnrm.getPayload();  
	}
	
	private byte[] CommunicateReadPrevToCS(RID rid, RID prevRid) {
		ReadPrevRecordMessage rprm = new ReadPrevRecordMessage(rid, prevRid);
		
		try {
			// Send the message
			csOos.writeObject(rprm);
			csOos.flush();
			
			// Receive the response and cast
			Object o = null;
			o = csOis.readObject();
			rprm = (ReadPrevRecordMessage)o;
			
			// Reset both streams
			csOos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in clientRec: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In ClientRec CommunicateAppendToCS " + cnfe.getMessage());
		}
		rid.setChunkHandle(rprm.getRid().getChunkHandle());
		rid.setSlotNumber(rprm.getRid().getSlotNumber());
		prevRid.setChunkHandle(rprm.getPrevRid().getChunkHandle());
		prevRid.setSlotNumber(rprm.getPrevRid().getSlotNumber());
		return rprm.getPayload();  
	}
}
