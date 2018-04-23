package com.chunkserver;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Vector;

import com.client.RID;
import com.message.AppendChunkToFileSpaceMessage;
import com.message.AppendRecordMessage;
import com.message.DeleteRecordMessage;
import com.message.GetChunkMessage;
import com.message.HeartBeatMessage;
import com.message.InformAppendRecordMessage;
import com.message.InformInitializeChunkMessage;
import com.message.InitializeChunkMessage;
import com.message.PutChunkMessage;
import com.message.ReadFirstRecordMessage;
import com.message.ReadLastRecordMessage;
import com.message.ReadNextRecordMessage;
import com.message.ReadPrevRecordMessage;

public class ChunkServerThread extends Thread{
	private ObjectOutputStream oos;
	private ObjectInputStream ois;
	private ChunkServer cs;
	
	public ChunkServerThread(Socket s, ChunkServer cs) {
		try {
			this.cs = cs;
			oos = new ObjectOutputStream(s.getOutputStream());
			ois = new ObjectInputStream(s.getInputStream());
			this.start();
		} catch (IOException ioe) {
			System.out.println("ioe in MasterThread constructor: "+ioe.getMessage());
		}
	}
	
	public void run() {
		try {
			while(true) {
				Object o = ois.readObject();
				
				if (o instanceof InitializeChunkMessage) {
					
					
					// Cast incoming message
					InitializeChunkMessage icm = (InitializeChunkMessage)o;
					
					// Get result from Master
					String ChunkHandle = cs.initializeChunk();
					
					System.out.println("Initializing Chunk!!! The name is " + ChunkHandle);
										
					// Create outgoing message
					InitializeChunkMessage outICM = new InitializeChunkMessage(ChunkHandle);
					
					// Write and flush
					oos.writeObject(outICM);
				} else if (o instanceof GetChunkMessage) {
					// Cast incoming message
					GetChunkMessage gcm = (GetChunkMessage)o;
					
					// Get parameters
					String ChunkHandle = gcm.getChunkHandle();
					int offset = gcm.getOffset();
					int numBytes = gcm.getNumberOfBytes(); 
					
					// Get result from Master
					byte[] payload = cs.getChunk(ChunkHandle, offset, numBytes);
										
					// Create outgoing message
					GetChunkMessage outGCM = new GetChunkMessage(payload);
					
					// Write and flush
					oos.writeObject(outGCM);
				} else if (o instanceof AppendRecordMessage) {
					// Cast incoming message
					AppendRecordMessage arm = (AppendRecordMessage)o;
					
					// Get parameters
					String ChunkHandle = arm.getChunkHandle();
					byte[] payload = arm.getPayload(); 
					String previousChunkHandle = arm.getPreviousChunkHandle();
					
					// Get result from Master
					Vector<RID> rids = cs.appendRecord(ChunkHandle, payload, previousChunkHandle);
										
					// Create outgoing message
					AppendRecordMessage outARM = new AppendRecordMessage(rids);
					
					// Write and flush
					oos.writeObject(outARM);
				} else if (o instanceof DeleteRecordMessage) {
					// Cast incoming message
					DeleteRecordMessage drm = (DeleteRecordMessage)o;
					
					// Get parameters
					RID rid = drm.getRid();
					String firstChunkHandle = drm.getFirstChunkHandle(); 
					
					// Get result from Master
					boolean isDeleted = cs.deleteRecord(rid, firstChunkHandle);
										
					// Create outgoing message
					DeleteRecordMessage outDRM = new DeleteRecordMessage(isDeleted);
					
					// Write and flush
					oos.writeObject(outDRM);
				} else if (o instanceof ReadFirstRecordMessage) {
					// Cast incoming message
					ReadFirstRecordMessage rfrm = (ReadFirstRecordMessage)o;
					
					// Get parameters
					String ChunkHandle = rfrm.getChunkHandle(); 
					RID rid = rfrm.getRid();
					
					// Get result from Master
					byte[] payload = cs.readFirstRecord(ChunkHandle, rid);
										
					// Create outgoing message
					ReadFirstRecordMessage outRFRM = new ReadFirstRecordMessage(payload, rid);
					
					// Write and flush
					oos.writeObject(outRFRM);
				} else if (o instanceof ReadLastRecordMessage) {
					// Cast incoming message
					ReadLastRecordMessage rlrm = (ReadLastRecordMessage)o;
					
					// Get parameters
					String ChunkHandle = rlrm.getChunkHandle(); 
					RID rid = rlrm.getRid();
					
					// Get result from Master
					byte[] payload = cs.readLastRecord(ChunkHandle, rid);
										
					// Create outgoing message
					ReadLastRecordMessage outRLRM = new ReadLastRecordMessage(payload, rid);
					
					// Write and flush
					oos.writeObject(outRLRM);
				} else if (o instanceof ReadNextRecordMessage) {
					// Cast incoming message
					ReadNextRecordMessage rnrm = (ReadNextRecordMessage)o;
					
					// Get parameters
					RID rid = rnrm.getRid();
					RID nextRid = rnrm.getNextRid(); 
					
					// Get result from Master
					byte[] payload = cs.readNextRecord(rid, nextRid); 
										
					// Create outgoing message
					ReadNextRecordMessage outRNRM = null;
					
					if (payload == null) {
						outRNRM = new ReadNextRecordMessage(rid, nextRid);
					} else {
						outRNRM = new ReadNextRecordMessage(payload, rid, nextRid);
					}					
					
					// Write and flush
					oos.writeObject(outRNRM);
				} else if (o instanceof ReadPrevRecordMessage) {
					// Cast incoming message
					ReadPrevRecordMessage rprm = (ReadPrevRecordMessage)o;
					
					// Get parameters
					RID rid = rprm.getRid();
					RID prevRid = rprm.getPrevRid(); 
					
					// Get result from Master
					byte[] payload = cs.readPrevRecord(rid, prevRid); 
										
					// Create outgoing message
					ReadPrevRecordMessage outRPRM = null;
					
					if (payload == null) {
						outRPRM = new ReadPrevRecordMessage(rid, prevRid);
					} else {
						outRPRM = new ReadPrevRecordMessage(payload, rid, prevRid);
					}	
					
					// Write and flush
					oos.writeObject(outRPRM);
				} else if (o instanceof InformAppendRecordMessage) {
					// Cast incoming message
					InformAppendRecordMessage iarm = (InformAppendRecordMessage)o;
					
					// Get parameters
					String ChunkHandle = iarm.getChunkHandle();
					byte[] payload = iarm.getPayload(); 
					String previousChunkHandle = iarm.getPreviousChunkHandle();
					
//					System.out.println("///");
//					System.out.println(cs);
//					System.out.println(payload);
//					System.out.println(ChunkHandle);
//					System.out.println(previousChunkHandle);
//					System.out.println("\\ \\ \n");
					
					// Get result from Master
					cs.appendRecord(ChunkHandle, payload, previousChunkHandle);
					
					oos.writeObject(new HeartBeatMessage()); 
									
				} else if (o instanceof InformInitializeChunkMessage) {
					
					
					// Cast incoming message
					InformInitializeChunkMessage iicm = (InformInitializeChunkMessage)o;
					
					// Get parameters
					String ChunkHandle = iicm.getChunkHandle();
					
					// Get result from Master
					cs.initializeChunk(ChunkHandle);
					
//					System.out.println("Initializing Chunk!!! The name is " + ChunkHandle);
										
					// Create outgoing message
					InitializeChunkMessage outICM = new InitializeChunkMessage(ChunkHandle);
					
					// Write and flush
					oos.writeObject(outICM);
				} else if (o instanceof PutChunkMessage) {
					// Cast incoming message
					PutChunkMessage pcm = (PutChunkMessage)o;
					
					// Get parameters
					String ChunkHandle = pcm.getChunkHandle();
					byte[] payload = pcm.getPayload(); 
					int offset = pcm.getOffset();
					
					// Get result from Master
					boolean status = cs.putChunk(ChunkHandle, payload, offset);
										
					// Create outgoing message
					PutChunkMessage outPCM = new PutChunkMessage(status);
					
					// Write and flush
					oos.writeObject(outPCM);
				}
			}
		} catch (IOException ioe) {
			System.out.println("mtRun ioe:"+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("mtRun cnfe:"+cnfe.getMessage());
		}
	}
}
