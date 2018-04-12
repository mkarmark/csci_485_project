package com.chunkserver;

import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Vector;

import com.client.RID;
import com.interfaces.ChunkServerInterface;

/**
 * implementation of interfaces at the chunkserver side
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer implements ChunkServerInterface {
	final static String filePath = "csci485/";	//or C:\\newfile.txt
	final static String portFilePath = "port.txt";
	public static long counter;
	
	//default path
	public static int port = 5656; 
	private static ServerSocket ss;
	
	public static void main(String[] args) {		
		ChunkServer cs = new ChunkServer(); 
		ss = null;
		boolean isPortNotFoundIssue = true;
		try {
			ss = new ServerSocket(port);
			Socket s; 
			
			//write 5656 to file 
			File portFile = new File(portFilePath);
			try {
				if (portFile.createNewFile()) {
					counter = 0; 
					BufferedWriter out = new BufferedWriter(new FileWriter(portFile)); 
					out.write("" + port); 
					out.close();
				}	else {
					BufferedWriter out = new BufferedWriter(new FileWriter(portFile, false)); 
					out.write("" + port); 
					out.close();
				}			
			} catch (IOException ioe) {
				System.out.println("IOException: " + ioe.getMessage()); 
			}
			
			//from here on out ioexception isn't because a port wasn't found 
			isPortNotFoundIssue = false;
			System.out.println("waiting for connection at port " + port + "...");  
			Vector<ServerThread> threads = new Vector<ServerThread>(); 
			//if any client tries to connect to the port, accept it 
			while (true) {
				s = ss.accept();
				System.out.println("Accepted");
				System.out.println("connection from " + s.getInetAddress() + ":" + s.getPort());
				ServerThread st = new ServerThread(s, cs);
				threads.add(st); 
			}
		} catch (EOFException eof) {
			System.out.println("UnitTest Closed"); 
		} catch (IOException ioe) {
			//if a port could not be found 
			if (isPortNotFoundIssue) {
				//increment port number by one until one is free
				int currPort = port + 1;
				boolean foundPort = false;
				while (!foundPort) {
					try {
						ss = new ServerSocket(currPort);
						foundPort = true;
						System.out.println("finally got a spot on port " + currPort);
					} catch (IOException ioe2) {
						//keep incrementing port number by 1 till you don't enter catch block
						currPort++;
						System.out.println("still stuck on occupied port");
					}
				}
				
				//write port number to file
				File portFile = new File(portFilePath);
				try {
					if (portFile.createNewFile()) {
						counter = 0; 
						BufferedWriter out = new BufferedWriter(new FileWriter(portFile)); 
						out.write("" + currPort); 
						out.close();
					}	else {
						BufferedWriter out = new BufferedWriter(new FileWriter(portFile, false)); 
						out.write("" + currPort); 
						out.close();
					}			
				} catch (IOException ioe2) {
					System.out.println("IOException: " + ioe2.getMessage()); 
				}
				
				try {
					Socket s; 	
					System.out.println("waiting for connection at port " + currPort + "...");  
					Vector<ServerThread> threads = new Vector<ServerThread>(); 
					
					//if any clients try to connect to the server socket accept the connection
					while (true) {
						s = ss.accept();
						System.out.println("Accepted");
						System.out.println("connection from " + s.getInetAddress() + ":" + s.getPort());
						ServerThread st = new ServerThread(s, cs);
						threads.add(st); 
					}
				} catch (EOFException eof) {
					System.out.println("UnitTest closed");
				} catch (IOException ioe2) {
					System.out.println("IOException: " + ioe2.getMessage());
				}
			}
		} 
	}
	
	/**
	 * Initialize the chunk server
	 */
	public ChunkServer(){
		ss = null; 
		File dir = new File(filePath);
		File[] fs = dir.listFiles();

		if(fs.length == 0){
			counter = 0;
		}else{
			long[] cntrs = new long[fs.length];
			for (int j=0; j < cntrs.length; j++)
				cntrs[j] = Long.valueOf( fs[j].getName() ); 
			
			Arrays.sort(cntrs);
			counter = cntrs[cntrs.length - 1];
		}
	}
	
	/**
	 * Each chunk is corresponding to a file.
	 * Return the chunk handle of the last chunk in the file.
	 */
	public String initializeChunk() {
		counter++;
		String chunkHandle = String.valueOf(counter); 
		try {
			RandomAccessFile raf = new RandomAccessFile(filePath + chunkHandle, "rw"); 
			ByteBuffer numSlots = ByteBuffer.allocate(4); 
			ByteBuffer nextFreeOffset = ByteBuffer.allocate(4); 
			numSlots.putInt(0);
			nextFreeOffset.putInt(8); 
			byte[] numSlotsArray = numSlots.array();
			byte[] nextFreeOffsetArray = nextFreeOffset.array(); 
			byte[] indexedHeap = new byte[ChunkServer.ChunkSize];
			int offset = 0;
			for (int i=0; i<numSlotsArray.length; i++) {
				indexedHeap[i] = numSlotsArray[i];
				offset++;
			}
			for (int i=0; i<nextFreeOffsetArray.length; i++) {
				indexedHeap[offset+i] = nextFreeOffsetArray[i];
			}
			raf.seek(0);
			raf.write(indexedHeap, 0, ChunkServer.ChunkSize);
		} catch (IOException ex) {
			
		}	
		return String.valueOf(counter);
	}
	
	/**
	 * Write the byte array to the chunk at the offset
	 * The byte array size should be no greater than 4KB
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		try {
			//If the file corresponding to ChunkHandle does not exist then create it before writing into it
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.write(payload, 0, payload.length);
			raf.close();
			return true;
		} catch (IOException ex) {
			ex.printStackTrace();
			return false;
		}
	}
	
	/**
	 * read the chunk at the specific offset
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		try {
			//If the file for the chunk does not exist the return null
			boolean exists = (new File(filePath + ChunkHandle)).exists();
			if (exists == false) return null;
			
			//File for the chunk exists then go ahead and read it
			byte[] data = new byte[NumberOfBytes];
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.read(data, 0, NumberOfBytes);
			raf.close();
			return data;
		} catch (IOException ex){
			ex.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Append payload to the end of the file 
	 * Create an extra chunk if the last chunk does not have space
	 * Returns a RID of the chunkhandles over which the byte payload was appended
	 */
	public RID appendRecord(String ChunkHandle, byte[] payload)
	{
		Vector<String> handles = new Vector<String>(); 
		handles.add(ChunkHandle); 
		boolean isChunkEnough = false;
		byte[] numSlots = getChunk(ChunkHandle, 0, 4);
		int intNumSlots = ByteBuffer.wrap(numSlots).getInt(); 
		byte[] nextFreeOffset = getChunk(ChunkHandle, 4, 4); 
		int intNextFreeOffset = ByteBuffer.wrap(nextFreeOffset).getInt(); 
		int numBytesInPayload = payload.length; 
		int numBytesFreeSpace = ChunkServer.ChunkSize - intNextFreeOffset - 4*(intNumSlots+1);
		//entire append can take place in last chunk 
		if (numBytesFreeSpace >= numBytesInPayload + 4)
		{
			//a new set of 4 bytes that indicate the number of slots
			intNumSlots++;
			ByteBuffer bNumSlots = ByteBuffer.allocate(4);
			bNumSlots.putInt(intNumSlots); 
			byte[] byteNumSlots = bNumSlots.array();
			
			//a new set of 4 bytes that indicate where the next free offset is after 
			//payload is placed in
			int currOffset = intNextFreeOffset;
			intNextFreeOffset += (4 + numBytesInPayload);
			ByteBuffer bNextFreeOffset = ByteBuffer.allocate(4);
			bNextFreeOffset.putInt(intNextFreeOffset);
			byte[] byteNextFreeOffset = bNextFreeOffset.array(); 
			
			//a header to the payload that indicates the size of the payload
			ByteBuffer bPayloadSize = ByteBuffer.allocate(4);
			bPayloadSize.putInt(numBytesInPayload);
			byte[] bytePayloadSize = bPayloadSize.array(); 
			
			//a new slot entry to add to the end of the chunk
			ByteBuffer bSlotEntry = ByteBuffer.allocate(4);
			bSlotEntry.putInt(currOffset);
			byte[] byteSlotEntry = bSlotEntry.array();
			
			//put all these pieces into the existing chunkhanlde
			putChunk(ChunkHandle, byteNumSlots, 0);
			putChunk(ChunkHandle, byteNextFreeOffset, 4);
			putChunk(ChunkHandle, bytePayloadSize, currOffset);
			putChunk(ChunkHandle, payload, currOffset + 4);
			putChunk(ChunkHandle, byteSlotEntry, ChunkServer.ChunkSize - 4*(intNumSlots));
			RID rid = new RID(ChunkHandle, intNumSlots);
			return rid; 
		} else {
			//create a new chunk handle
			String newChunkHandle = initializeChunk();
			//call appendRecord on that chunkhandle:
			return appendRecord(newChunkHandle, payload);
		}
	}
	
	public void deleteRecord(RID rid) {
		String ChunkHandle = rid.getChunkHandle();
		int slotNumber = rid.getSlotNumber();
		
		byte[] numOffsets = getChunk(ChunkHandle, 0, 4);
		int intNumOffsets = ByteBuffer.wrap(numOffsets).getInt(); 
		
		byte[] offsetToDelete = getChunk(ChunkHandle, ChunkServer.ChunkSize - 4*(slotNumber), 4);
		int intOffSetToDelete = ByteBuffer.wrap(offsetToDelete).getInt();
		
		byte[] numBytesInDeletedPayload = getChunk(ChunkHandle, intOffSetToDelete, 4);
		int intNumBytesInDeletedPayload = ByteBuffer.wrap(numBytesInDeletedPayload).getInt(); 
		int shiftingFactor = intNumBytesInDeletedPayload + 4; 
		
		//set currSlot to -1
		int newOffset = -1;
		ByteBuffer b = ByteBuffer.allocate(4);
		byte[] bytesNewOffset = b.putInt(newOffset).array();
		putChunk(ChunkHandle, bytesNewOffset, 4); 
		
		//clear space
		for (int i=slotNumber+1; i<=intNumOffsets; i++) {
			byte[] offset = getChunk(ChunkHandle, ChunkServer.ChunkSize - 4*i, 4); 
			int intOffset = ByteBuffer.wrap(offset).getInt(); 
			if (intOffset != -1) {
				byte[] numBytes = getChunk(ChunkHandle, intOffset, 4);
				int intNumBytes = ByteBuffer.wrap(numBytes).getInt(); 
				byte[] payload = getChunk(ChunkHandle, intOffset+4, intNumBytes);
				//shift it and numBytes by shifting factor
				putChunk(ChunkHandle, numBytes, intOffset - shiftingFactor);
				putChunk(ChunkHandle, payload, intOffset + 4 - shiftingFactor);
			}
		}
		
		//shift next available offset
		byte[] nextAvailableOffset = getChunk(ChunkHandle, 4, 4);
		int intNextAvailableOffset = ByteBuffer.wrap(nextAvailableOffset).getInt();
		intNextAvailableOffset -= shiftingFactor;
		ByteBuffer bIntNextAvailableOffset = ByteBuffer.allocate(4);
		byte[] newNextAvailableOffset = bIntNextAvailableOffset.putInt(intNextAvailableOffset).array();
		putChunk(ChunkHandle, newNextAvailableOffset, 4); 
	}
	
	public byte[] readFirstRecord(String ChunkHandle) {
		byte[] numSlots = getChunk(ChunkHandle, 0, 4); 
		int intNumSlots = ByteBuffer.wrap(numSlots).getInt();
		if (intNumSlots == 0) {
			return null; 
		} else {
			byte[] offset = getChunk(ChunkHandle, ChunkServer.ChunkSize - 4, 4);
			int intOffset = ByteBuffer.wrap(offset).getInt(); 
			byte[] size = getChunk(ChunkHandle, intOffset, 4);
			int intSize = ByteBuffer.wrap(size).getInt(); 
			return getChunk(ChunkHandle, intOffset+4, intSize); 
		}
	}


	public byte[] readLastRecord(String ChunkHandle) {
		byte[] numSlots = getChunk(ChunkHandle, 0, 4); 
		int intNumSlots = ByteBuffer.wrap(numSlots).getInt();
		if (intNumSlots == 0) {
			return null; 
		} else {
			byte[] offset = getChunk(ChunkHandle, ChunkServer.ChunkSize - 4*intNumSlots, 4);
			int intOffset = ByteBuffer.wrap(offset).getInt(); 
			byte[] size = getChunk(ChunkHandle, intOffset, 4);
			int intSize = ByteBuffer.wrap(size).getInt(); 
			return getChunk(ChunkHandle, intOffset+4, intSize); 
		}
	}
	
	public byte[] readNextRecord(RID rid) {
		String ChunkHandle = rid.getChunkHandle();
		int slotNumber = rid.getSlotNumber();
		byte[] numSlots = getChunk(ChunkHandle, 0, 4); 
		int intNumSlots = ByteBuffer.wrap(numSlots).getInt();
		if (slotNumber == intNumSlots) {
			return null;
		} else {
			int nextSlotNumber = slotNumber+1;
			byte[] offset = getChunk(ChunkHandle, ChunkServer.ChunkSize - 4*nextSlotNumber, 4);
			int intOffset = ByteBuffer.wrap(offset).getInt(); 
			byte[] size = getChunk(ChunkHandle, intOffset, 4);
			int intSize = ByteBuffer.wrap(size).getInt(); 
			return getChunk(ChunkHandle, intOffset+4, intSize); 
		}
	}
	
	public byte[] readPrevRecord(RID rid) {
		String ChunkHandle = rid.getChunkHandle();
		int slotNumber = rid.getSlotNumber();
		if (slotNumber == 1) {
			return null;
		} else {
			int nextSlotNumber = slotNumber-1;
			byte[] offset = getChunk(ChunkHandle, ChunkServer.ChunkSize - 4*nextSlotNumber, 4);
			int intOffset = ByteBuffer.wrap(offset).getInt(); 
			byte[] size = getChunk(ChunkHandle, intOffset, 4);
			int intSize = ByteBuffer.wrap(size).getInt(); 
			return getChunk(ChunkHandle, intOffset+4, intSize); 
		}
	}
}