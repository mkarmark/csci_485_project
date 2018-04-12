package com.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
	final static String portFilePath = "port.txt";
	
	private Socket s; 
	private PrintWriter pw; 
	private BufferedReader br;
	/**
	 * Initialize the client
	 */
	public Client(){
		//default port number
		int portNumber = 5656;
		
		//read port number from file
		File portFile = new File(portFilePath);
		try {
			if (!portFile.createNewFile()) {
				BufferedReader in = new BufferedReader(new FileReader(portFile));
				String line = in.readLine(); 
				in.close(); 
				portNumber = Integer.parseInt(line); 
			}			
		} catch (IOException ioe) {
			System.out.println("IOException: " + ioe.getMessage()); 
		}
		
		//set up connection with server
		try {
			s = new Socket("localhost", portNumber); 
			pw = new PrintWriter(s.getOutputStream()); 
			br = new BufferedReader(new InputStreamReader(s.getInputStream())); 
		} catch (IOException ioe) {
			
		}
	}
	
	/**
	 * Create a chunk at the chunk server from the client side.
	 */
	public String initializeChunk() {
		//send an initialize command to chunk server
		pw.println("initialize");
		pw.flush(); 
		try {
			//read handle from chunk server and return it
			String handle = br.readLine(); 
			return handle; 
		} catch (IOException ioe) {
			System.out.println("Client initialize chunk ioe: " + ioe.getMessage()); 
		}
		return "";
	}
	
	/**
	 * Write a chunk at the chunk server from the client side.
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		if(offset + payload.length > Client.ChunkSize){
			System.out.println("The chunk write should be within the range of the file, invalide chunk write!");
			return false;
		}
		
		//create packet in string form with chunkhandle, offset, payload length, and payload in string form
		String packet = "PUT: " + ChunkHandle + " " + offset + " " + payload.length + " [";
		for (int i = 0; i< payload.length - 1; i++) {
			packet += payload[i] + ","; 
		}
		packet += payload[payload.length-1] + "]"; 
		//send packet over to chunkserver
		pw.println(packet);
		pw.flush(); 
		try {
			//read status as string from server and return boolean accordingly 
			String status = br.readLine();  
			if (status.equals("true")) {
				return true;
			} else {
				return false; 
			}
		} catch (IOException ioe) {
			System.out.println("Client put chunk ioe: " + ioe.getMessage()); 
		}
		return false; 
		//return cs.putChunk(ChunkHandle, payload, offset);
	}
	
	/**
	 * Read a chunk at the chunk server from the client side.
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		if(NumberOfBytes + offset > Client.ChunkSize){
			System.out.println("The chunk read should be within the range of the file, invalide chunk read!");
			return null;
		}
		//set up packet with chunk handle, offset, and number of bytes to get and send it to the client
		String packet = "GET: " + ChunkHandle + " " + offset + " " + NumberOfBytes; 
		pw.println(packet);
		pw.flush(); 
		try {
			//read the payload and parse the string as a bytes array
			String payload = br.readLine(); 
			payload = payload.substring(2, payload.length()-1);
			Scanner payloadTemp = new Scanner(payload); 
			Scanner payloadScanner = payloadTemp.useDelimiter(","); 
			byte[] payloadArray = new byte[NumberOfBytes]; 
			for (int i=0; i<NumberOfBytes; i++) {
				payloadArray[i] = payloadScanner.nextByte();
			}
			payloadTemp.close();
			payloadScanner.close();
			//return the array of bytes
			return payloadArray; 
		} catch (IOException ioe) {
			System.out.println("Client get chunk ioe: " + ioe.getMessage()); 
		}
		return null; 
	}
}
