package com.chunkserver;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import com.message.AppendChunkToFileSpaceMessage;
import com.message.InitializeChunkMessage;

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
										
					// Create outgoing message
					InitializeChunkMessage outICM = new InitializeChunkMessage(ChunkHandle);
					
					// Write and flush
					oos.writeObject(outICM);
				}
			}
		} catch (IOException ioe) {
			System.out.println("mtRun ioe:"+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("mtRun cnfe:"+cnfe.getMessage());
		}
	}
}
