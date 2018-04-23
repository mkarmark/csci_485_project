package com.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Scanner;
import java.util.Vector;

import com.chunkserver.Location;
import com.interfaces.ClientInterface;
import com.message.GetChunkMessage;
import com.message.InitializeChunkMessage;
import com.message.PutChunkMessage;
import com.message.RequestChunkServerLocationsMessage;

/**
 * implementation of interfaces at the client side
 * @author Shahram Ghandeharizadeh
 *
 */
public class Client implements ClientInterface {
//	final static String filePath = "";
	private ObjectInputStream csOis;
	private ObjectOutputStream csOos;
	private ObjectInputStream msOis;
	private ObjectOutputStream msOos; 
	
	/**
	 * Initialize the client
	 */
	public Client(){
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
		Vector<Location> locations = rcslm.getLocations();  
		
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
	 * Create a chunk at the chunk server from the client side.
	 */
	public String initializeChunk() {
		InitializeChunkMessage icm = new InitializeChunkMessage(); 
		try{
			csOos.writeObject(icm);
			csOos.flush();
			
			// Read in the chunk handle
			icm = (InitializeChunkMessage)csOis.readObject();
			
			// Reset output stream
			csOos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in client: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In Client initialize chunk " + cnfe.getMessage());
		}
		
		return icm.getChunkHandle();
	}
	
	/**
	 * Write a chunk at the chunk server from the client side.
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		if (offset + payload.length > Client.ChunkSize) {
			return false; 
		}
		
		PutChunkMessage pcm = new PutChunkMessage(ChunkHandle, payload, offset);
		
		boolean success = false;
		
		try{
			csOos.writeObject(pcm);
			csOos.flush();
			
			pcm = (PutChunkMessage)csOis.readObject();
			
			// Reset object output stream
			csOos.reset();
			
		} catch (IOException ioe) {
			System.out.println("ioe in client: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In Client initialize chunk " + cnfe.getMessage());
		}
		
		return pcm.getStatus();
	}
	
	/**
	 * Read a chunk at the chunk server from the client side.
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		if(NumberOfBytes + offset > Client.ChunkSize){
			System.out.println("The chunk read should be within the range of the file, invalide chunk read!");
			return null;
		}
		
		GetChunkMessage gcm = new GetChunkMessage(ChunkHandle, offset, NumberOfBytes); 
		
		byte[] result = null;
		
		try{
			csOos.writeObject(gcm);
			csOos.flush();
			
			// Determine success from string to boolean
			gcm = (GetChunkMessage)csOis.readObject();
			
			// Reset Output Stream
			csOos.reset();
			
		} catch (IOException ioe) {
			System.out.println("ioe in client: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In Client initialize chunk " + cnfe.getMessage());
		}
		
		return gcm.getPayload();
	}
}
