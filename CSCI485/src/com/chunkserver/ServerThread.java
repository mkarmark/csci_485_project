package com.chunkserver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class ServerThread extends Thread {
	private PrintWriter pw;
	private BufferedReader br;
	private ChunkServer cs;
	
	public ServerThread(Socket s, ChunkServer cs) {
		try {
			this.cs = cs;
			//set up writer and reader 
			pw = new PrintWriter(s.getOutputStream());
			br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			this.start();
		} catch (IOException ioe) {
			System.out.println("ioe in ServerThread constructor: " + ioe.getMessage());
		}
	}
	
	public void run() {
		try {
			while (true) {
				String line = br.readLine();
				//if the line says initialize, initialize the chunk server
				//pass the handle to the client
				if (line.equals("initialize")) {
					String handle = cs.initializeChunk(); 
					pw.println(handle);
					pw.flush(); 
				//if the line starts with put, read the rest of the line and put payload in chunkserver
				} else if (line.startsWith("PUT")) {
					Scanner lineScan = new Scanner(line); 
					lineScan.next();
					//first string is handle
					String handle = lineScan.next();
					
					//second token is int offset
					int offset = lineScan.nextInt();
					
					//next token is int size
					int size = lineScan.nextInt();
					
					//final token is payload
					String payload = lineScan.next(); 
					//take bracket off from either end
					payload = payload.substring(1, payload.length()-1);
					Scanner payloadTemp = new Scanner(payload);
					//delimiter is comma ,
					Scanner payloadScanner = payloadTemp.useDelimiter(","); 
					//read payload into array of bytes
					byte[] payloadArray = new byte[size]; 
					for (int i=0; i<size; i++) {
						payloadArray[i] = payloadScanner.nextByte(); 
					}
					//pass the response to the client
					boolean response = cs.putChunk(handle, payloadArray, offset);
					pw.println(response);
					pw.flush(); 
					lineScan.close(); 
					payloadTemp.close();
					payloadScanner.close(); 
				//if the line starts with get, read the rest of the line and get payload from chunkserver
				} else if (line.startsWith("GET")) {
					Scanner lineScan = new Scanner(line); 
					lineScan.next(); 
					//read handle, offset, and size from line
					String handle = lineScan.next();
					int offset = lineScan.nextInt();
					int size = lineScan.nextInt(); 
					//format payload as a string and pass it to client
					byte[] payload = cs.getChunk(handle, offset, size);
					String packet = " [";
					for (int i = 0; i< payload.length - 1; i++) {
						packet += payload[i] + ","; 
					}
					packet += payload[payload.length-1] + "]"; 
					pw.println(packet);
					pw.flush();
					lineScan.close();
				}
			}
		} catch (IOException ioe) {
			System.out.println("ioe in ServerThread.run(): " + ioe.getMessage());
		}
	}
}
