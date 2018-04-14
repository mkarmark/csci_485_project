package com.chunkserver;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import com.client.ChunkMessage;

public class ServerThread extends Thread {

	private ObjectOutputStream oos;
	private ObjectInputStream ois;
	private ChunkServer cs;
	
	public ServerThread(Socket s, ChunkServer cs) {
		try {
			this.cs = cs;
			oos = new ObjectOutputStream(s.getOutputStream());
			ois = new ObjectInputStream(s.getInputStream());
			this.start();
		} catch (IOException ioe) {
			System.out.println("ioe in ChunkThread constructor: "+ioe.getMessage());
		}
	}
	
	public void sendHandle(String chunkHandle)
	{
		try {
			oos.writeObject(chunkHandle);
			oos.flush();
		} catch (IOException ioe) {
			System.out.println("In Send Handle ioe: "+ioe.getMessage());
		}
	}
	
	public void sendChunk(byte[] chunk)
	{
		try {
			oos.writeObject(chunk);
			oos.flush();
		} catch (IOException ioe) {
			System.out.println("ChunkThread sendChunk ioe: "+ioe.getMessage());
		}
	}
	
	public void sendSuccess(boolean success)
	{
		try {
			if(success){
				oos.writeObject("True");
			} else {
				oos.writeObject("False");
			}
			oos.flush();
		} catch (IOException ioe) {
			System.out.println("ChunkThread sendSuccess ioe: "+ioe.getMessage());
		}
	}
	
	public void run() {
		try {
			while(true) {
				ChunkMessage c = (ChunkMessage)ois.readObject();
				switch (c.getType()) {
					case 1:
						sendHandle(cs.initializeChunk());
						break;
					case 2:
						sendChunk(cs.getChunk(c.getHandle(), c.getOffset(), c.getSize()));
						break;
					case 3: 
						sendSuccess(cs.putChunk(c.getHandle(), c.getChunk(), c.getOffset()));
						break;
				}
			}
		} catch (IOException ioe) {
//			System.out.println("ioe in ChunkThread.run(): " + ioe.getMessage());
			// Do nothing
		}
		catch (ClassNotFoundException cnfe) {
			System.out.println("cnfe: " + cnfe.getMessage());
		}
	}
}
