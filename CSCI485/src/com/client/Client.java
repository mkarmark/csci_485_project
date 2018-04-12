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
import java.net.Socket;
import java.util.Scanner;

import com.interfaces.ClientInterface;

/**
 * implementation of interfaces at the client side
 * @author Shahram Ghandeharizadeh
 *
 */
public class Client implements ClientInterface {
	final static String filePath = "/Users/Nandhini/Documents/CSCI485/NewFiles/";
	private ObjectInputStream ois;
	private ObjectOutputStream oos;
	
	/**
	 * Initialize the client
	 */
	public Client(){
		int port = 5858;
		
		// Get the port number
		File portFile = new File(filePath+"port.txt");
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
			Socket s = new Socket("localhost", port);
			
			ois = new ObjectInputStream(s.getInputStream());
			oos = new ObjectOutputStream(s.getOutputStream());
		} catch (IOException ioe) {
			System.out.println("ioe in client constructor: " + ioe.getMessage());
		}
	}
	
	/**
	 * Create a chunk at the chunk server from the client side.
	 */
	public String initializeChunk() {
		ChunkMessage cm = new ChunkMessage(0, 1);
		String chunkHandle = "";
		try{
			oos.writeObject(cm);
			oos.flush();
			
			// Read in the chunk handle
			chunkHandle = (String)ois.readObject();
			
			// Reset output stream
			oos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in client: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In Client initialize chunk " + cnfe.getMessage());
		}
		
		return chunkHandle;
	}
	
	/**
	 * Write a chunk at the chunk server from the client side.
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		if(offset + payload.length > Client.ChunkSize){
			System.out.println("The chunk write should be within the range of the file, invalide chunk write!");
			return false;
		}
		
		// Create the chunk message
		ChunkMessage cm = new ChunkMessage(0,3);
		cm.setOffset(offset);
		cm.setChunk(payload);
		cm.setHandle(ChunkHandle);
		
		boolean success = false;
		
		try{
			oos.writeObject(cm);
			oos.flush();
			
			// Determine success from string to boolean
			String result = (String)ois.readObject();
			if(result.equals("True"))
			{
				success = true;
			}
			
			// Reset object output stream
			oos.reset();
			
		} catch (IOException ioe) {
			System.out.println("ioe in client: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In Client initialize chunk " + cnfe.getMessage());
		}
		
		return success;
	}
	
	/**
	 * Read a chunk at the chunk server from the client side.
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		if(NumberOfBytes + offset > Client.ChunkSize){
			System.out.println("The chunk read should be within the range of the file, invalide chunk read!");
			return null;
		}
		
		// Create the chunk message
		ChunkMessage cm = new ChunkMessage(NumberOfBytes,2);
		cm.setOffset(offset);
		cm.setHandle(ChunkHandle);
		
		byte[] result = null;
		
		try{
			oos.writeObject(cm);
			oos.flush();
			
			// Determine success from string to boolean
			result = (byte[])ois.readObject();
			
			// Reset Output Stream
			oos.reset();
			
		} catch (IOException ioe) {
			System.out.println("ioe in client: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In Client initialize chunk " + cnfe.getMessage());
		}
		
		return result;
	}
}
