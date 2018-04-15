package com.chunkserver;

import java.io.BufferedWriter; 
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Scanner;
import java.util.Vector;

import com.client.RID;
import com.interfaces.ChunkServerInterface;
//
///**
// * implementation of interfaces at the chunkserver side
// * @author Shahram Ghandeharizadeh
// *
// */
//
//public class ChunkServer implements ChunkServerInterface {
////	final static String filePath = "csci485/"; // For Windows
//	final static String filePath = "/Users/Nandhini/Documents/CSCI485/NewFiles/"; // For Nandhini's Mac
//	final static String portFilePath = "port.txt";
//	public static long counter;
//	
//	/**
//	 * Initialize the chunk server
//	 */
//	public ChunkServer(){
//		ServerSocket ss; 
//		boolean havePort = false;
//		int port = 5858;
//		
//		// Check if a metadata file exists and read counter from it
//		File metadata = new File(filePath+"metadata.txt");
//		Scanner scanner;
//		try {
//			scanner = new Scanner(metadata);
//			while(scanner.hasNext())
//			{
//				counter = scanner.nextInt();
//			}
//			scanner.close();
//		} catch (FileNotFoundException e) {
//			counter = 0;
//			// If the file doesn't exist, create it
//			BufferedWriter writer;
//			try {
//				writer = new BufferedWriter(new FileWriter(filePath+"metadata.txt"));
//				writer.write(""+counter);
//			    writer.close();
//			} catch (IOException e1) {
//				e1.printStackTrace();
//			}
//		}
//		
//		// Try to connect to a port
//		while(!havePort)
//		{
//			try
//			{
//				ss = new ServerSocket(port);
//				
//				// Write port number out to file
//				BufferedWriter writer;
//				try {
//					writer = new BufferedWriter(new FileWriter(filePath+"port.txt"));
//					writer.write(""+port);
//				    writer.close();
//				} catch (IOException e1) {
//					e1.printStackTrace();
//				}
//				
//				// Loop to accept connections
//				while(true) {
//					Socket s = ss.accept();
//					ServerThread ct = new ServerThread(s, this);
//				}
//			}
//			catch (IOException ioe)
//			{
//				port++;
//			}
//		}
//	}
//	
//	/**
//	 * Each chunk is corresponding to a file.
//	 * Return the chunk handle of the last chunk in the file.
//	 */
//	public String initializeChunk() {
//		counter++;
//		
//		// Write to metadata file
//		BufferedWriter out = null;
//		try {
//			out = new BufferedWriter(new FileWriter(filePath+"metadata.txt"));
//			out.write(""+counter);
//			out.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		String chunkHandle = String.valueOf(counter); 
//		try {
//			RandomAccessFile raf = new RandomAccessFile(filePath + chunkHandle, "rw"); 
//			ByteBuffer numSlots = ByteBuffer.allocate(4); 
//			ByteBuffer nextFreeOffset = ByteBuffer.allocate(4); 
//			numSlots.putInt(0);
//			nextFreeOffset.putInt(8); 
//			byte[] numSlotsArray = numSlots.array();
//			byte[] nextFreeOffsetArray = nextFreeOffset.array(); 
//			byte[] indexedHeap = new byte[ChunkServer.ChunkSize];
//			int offset = 0;
//			for (int i=0; i<numSlotsArray.length; i++) {
//				indexedHeap[i] = numSlotsArray[i];
//				offset++;
//			}
//			for (int i=0; i<nextFreeOffsetArray.length; i++) {
//				indexedHeap[offset+i] = nextFreeOffsetArray[i];
//			}
//			raf.seek(0);
//			raf.write(indexedHeap, 0, ChunkServer.ChunkSize);
//		} catch (IOException ex) {
//			
//		}	
//		
//		return String.valueOf(counter);
//	}
//	
//	/**
//	 * Write the byte array to the chunk at the offset
//	 * The byte array size should be no greater than 4KB
//	 */
//	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
//		try {
//			//If the file corresponding to ChunkHandle does not exist then create it before writing into it
//			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
//			raf.seek(offset);
//			raf.write(payload, 0, payload.length);
//			raf.close();
//			return true;
//		} catch (IOException ex) {
//			ex.printStackTrace();
//			return false;
//		}
//	}
//	
//	/**
//	 * read the chunk at the specific offset
//	 */
//	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
//		try {
//			//If the file for the chunk does not exist the return null
//			boolean exists = (new File(filePath + ChunkHandle)).exists();
//			if (exists == false) return null;
//			
//			//File for the chunk exists then go ahead and read it
//			byte[] data = new byte[NumberOfBytes];
//			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
//			raf.seek(offset);
//			raf.read(data, 0, NumberOfBytes);
//			raf.close();
//			return data;
//		} catch (IOException ex){
//			ex.printStackTrace();
//			return null;
//		}
//	}
//	
//	/**
//	 * Append payload to the end of the file 
//	 * Create an extra chunk if the last chunk does not have space
//	 * Returns a RID of the chunkhandles over which the byte payload was appended
//	 */
//	public RID appendRecord(String ChunkHandle, byte[] payload)
//	{
//		Vector<String> handles = new Vector<String>(); 
//		handles.add(ChunkHandle); 
//		boolean isChunkEnough = false;
//		byte[] numSlots = getChunk(ChunkHandle, 0, 4);
//		int intNumSlots = ByteBuffer.wrap(numSlots).getInt(); 
//		byte[] nextFreeOffset = getChunk(ChunkHandle, 4, 4); 
//		int intNextFreeOffset = ByteBuffer.wrap(nextFreeOffset).getInt(); 
//		int numBytesInPayload = payload.length; 
//		int numBytesFreeSpace = ChunkServer.ChunkSize - intNextFreeOffset - 4*(intNumSlots+1);
//		//entire append can take place in last chunk 
//		if (numBytesFreeSpace >= numBytesInPayload + 4)
//		{
//			//a new set of 4 bytes that indicate the number of slots
//			intNumSlots++;
//			ByteBuffer bNumSlots = ByteBuffer.allocate(4);
//			bNumSlots.putInt(intNumSlots); 
//			byte[] byteNumSlots = bNumSlots.array();
//			
//			//a new set of 4 bytes that indicate where the next free offset is after 
//			//payload is placed in
//			int currOffset = intNextFreeOffset;
//			intNextFreeOffset += (4 + numBytesInPayload);
//			ByteBuffer bNextFreeOffset = ByteBuffer.allocate(4);
//			bNextFreeOffset.putInt(intNextFreeOffset);
//			byte[] byteNextFreeOffset = bNextFreeOffset.array(); 
//			
//			//a header to the payload that indicates the size of the payload
//			ByteBuffer bPayloadSize = ByteBuffer.allocate(4);
//			bPayloadSize.putInt(numBytesInPayload);
//			byte[] bytePayloadSize = bPayloadSize.array(); 
//			
//			//a new slot entry to add to the end of the chunk
//			ByteBuffer bSlotEntry = ByteBuffer.allocate(4);
//			bSlotEntry.putInt(currOffset);
//			byte[] byteSlotEntry = bSlotEntry.array();
//			
//			//put all these pieces into the existing chunkhanlde
//			putChunk(ChunkHandle, byteNumSlots, 0);
//			putChunk(ChunkHandle, byteNextFreeOffset, 4);
//			putChunk(ChunkHandle, bytePayloadSize, currOffset);
//			putChunk(ChunkHandle, payload, currOffset + 4);
//			putChunk(ChunkHandle, byteSlotEntry, ChunkServer.ChunkSize - 4*(intNumSlots));
//			RID rid = new RID(ChunkHandle, intNumSlots);
//			return rid; 
//		} else {
//			//create a new chunk handle
//			String newChunkHandle = initializeChunk();
//			//call appendRecord on that chunkhandle:
//			return appendRecord(newChunkHandle, payload);
//		}
//	}
//	
//	public void deleteRecord(RID rid) {
//		String ChunkHandle = rid.getChunkHandle();
//		int slotNumber = rid.getSlotNumber();
//		
//		byte[] numOffsets = getChunk(ChunkHandle, 0, 4);
//		int intNumOffsets = ByteBuffer.wrap(numOffsets).getInt(); 
//		
//		byte[] offsetToDelete = getChunk(ChunkHandle, ChunkServer.ChunkSize - 4*(slotNumber), 4);
//		int intOffSetToDelete = ByteBuffer.wrap(offsetToDelete).getInt();
//		
//		byte[] numBytesInDeletedPayload = getChunk(ChunkHandle, intOffSetToDelete, 4);
//		int intNumBytesInDeletedPayload = ByteBuffer.wrap(numBytesInDeletedPayload).getInt(); 
//		int shiftingFactor = intNumBytesInDeletedPayload + 4; 
//		
//		//set currSlot to -1
//		int newOffset = -1;
//		ByteBuffer b = ByteBuffer.allocate(4);
//		byte[] bytesNewOffset = b.putInt(newOffset).array();
//		putChunk(ChunkHandle, bytesNewOffset, 4); 
//		
//		//clear space
//		for (int i=slotNumber+1; i<=intNumOffsets; i++) {
//			byte[] offset = getChunk(ChunkHandle, ChunkServer.ChunkSize - 4*i, 4); 
//			int intOffset = ByteBuffer.wrap(offset).getInt(); 
//			if (intOffset != -1) {
//				byte[] numBytes = getChunk(ChunkHandle, intOffset, 4);
//				int intNumBytes = ByteBuffer.wrap(numBytes).getInt(); 
//				byte[] payload = getChunk(ChunkHandle, intOffset+4, intNumBytes);
//				//shift it and numBytes by shifting factor
//				putChunk(ChunkHandle, numBytes, intOffset - shiftingFactor);
//				putChunk(ChunkHandle, payload, intOffset + 4 - shiftingFactor);
//			}
//		}
//		
//		//shift next available offset
//		byte[] nextAvailableOffset = getChunk(ChunkHandle, 4, 4);
//		int intNextAvailableOffset = ByteBuffer.wrap(nextAvailableOffset).getInt();
//		intNextAvailableOffset -= shiftingFactor;
//		ByteBuffer bIntNextAvailableOffset = ByteBuffer.allocate(4);
//		byte[] newNextAvailableOffset = bIntNextAvailableOffset.putInt(intNextAvailableOffset).array();
//		putChunk(ChunkHandle, newNextAvailableOffset, 4); 
//	}
//	
//	public byte[] readFirstRecord(String ChunkHandle) {
//		byte[] numSlots = getChunk(ChunkHandle, 0, 4); 
//		int intNumSlots = ByteBuffer.wrap(numSlots).getInt();
//		if (intNumSlots == 0) {
//			return null; 
//		} else {
//			byte[] offset = getChunk(ChunkHandle, ChunkServer.ChunkSize - 4, 4);
//			int intOffset = ByteBuffer.wrap(offset).getInt(); 
//			byte[] size = getChunk(ChunkHandle, intOffset, 4);
//			int intSize = ByteBuffer.wrap(size).getInt(); 
//			return getChunk(ChunkHandle, intOffset+4, intSize); 
//		}
//	}
//
//
//	public byte[] readLastRecord(String ChunkHandle) {
//		byte[] numSlots = getChunk(ChunkHandle, 0, 4); 
//		int intNumSlots = ByteBuffer.wrap(numSlots).getInt();
//		if (intNumSlots == 0) {
//			return null; 
//		} else {
//			byte[] offset = getChunk(ChunkHandle, ChunkServer.ChunkSize - 4*intNumSlots, 4);
//			int intOffset = ByteBuffer.wrap(offset).getInt(); 
//			byte[] size = getChunk(ChunkHandle, intOffset, 4);
//			int intSize = ByteBuffer.wrap(size).getInt(); 
//			return getChunk(ChunkHandle, intOffset+4, intSize); 
//		}
//	}
//	
//	public byte[] readNextRecord(RID rid) {
//		String ChunkHandle = rid.getChunkHandle();
//		int slotNumber = rid.getSlotNumber();
//		byte[] numSlots = getChunk(ChunkHandle, 0, 4); 
//		int intNumSlots = ByteBuffer.wrap(numSlots).getInt();
//		if (slotNumber == intNumSlots) {
//			return null;
//		} else {
//			int nextSlotNumber = slotNumber+1;
//			byte[] offset = getChunk(ChunkHandle, ChunkServer.ChunkSize - 4*nextSlotNumber, 4);
//			int intOffset = ByteBuffer.wrap(offset).getInt(); 
//			byte[] size = getChunk(ChunkHandle, intOffset, 4);
//			int intSize = ByteBuffer.wrap(size).getInt(); 
//			return getChunk(ChunkHandle, intOffset+4, intSize); 
//		}
//	}
//	
//	public byte[] readPrevRecord(RID rid) {
//		String ChunkHandle = rid.getChunkHandle();
//		int slotNumber = rid.getSlotNumber();
//		if (slotNumber == 1) {
//			return null;
//		} else {
//			int nextSlotNumber = slotNumber-1;
//			byte[] offset = getChunk(ChunkHandle, ChunkServer.ChunkSize - 4*nextSlotNumber, 4);
//			int intOffset = ByteBuffer.wrap(offset).getInt(); 
//			byte[] size = getChunk(ChunkHandle, intOffset, 4);
//			int intSize = ByteBuffer.wrap(size).getInt(); 
//			return getChunk(ChunkHandle, intOffset+4, intSize); 
//		}
//	}
//	
//	/** Main function **/
//	public static void main(String [] args) {
//		// Put new ServerSocket() in a while loop incrementing by 1 in catch until 
//		// it binds. Write the port number into a file that client can read from
//		ChunkServer cs = new ChunkServer();
//	}
//}

/**
 * implementation of interfaces at the chunkserver side
 * 
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer implements ChunkServerInterface {
//	final static String filePath = "C:\\Users\\shahram\\Documents\\TinyFS-2\\csci485Disk\\"; // or C:\\newfile.txt
	final static String filePath = "/Users/Nandhini/Documents/CSCI485/NewFiles/";
//	final static String filePath = "C:\\Users\\mital\\Desktop\\csci485folder\\";
	public static long counter;

	/**
	 * Initialize the chunk server
	 */
	public ChunkServer() {
		// Check if a metadata file exists and read counter from it
		File metadata = new File(filePath+"metadata.txt");
		Scanner scanner;
		try {
			scanner = new Scanner(metadata);
			while(scanner.hasNext())
			{
				counter = scanner.nextInt();
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			counter = 0;
			// If the file doesn't exist, create it
			BufferedWriter writer;
			try {
				writer = new BufferedWriter(new FileWriter(filePath+"metadata.txt"));
				writer.write(""+counter);
			    writer.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			
		}
			
	}


	/**
	 * Each chunk is corresponding to a file.
	 * Return the chunk handle of the last chunk in the file.
	 */
	public String initializeChunk() {
		counter++;
		
		// Write to metadata file
		BufferedWriter out = null;
		try {
			out = new BufferedWriter(new FileWriter(filePath+"metadata.txt"));
			out.write(""+counter);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String chunkHandle = String.valueOf(counter); 
		
		// Create the file
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
	
	public boolean deleteRecord(RID rid) {
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
		putChunk(ChunkHandle, bytesNewOffset, ChunkServer.ChunkSize - 4*(slotNumber)); 
		
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
				int changedSlotValue = intOffset - shiftingFactor; 
				ByteBuffer bChangedSlotValue = ByteBuffer.allocate(4);
				byte[] bytesChangedSlotValue = bChangedSlotValue.putInt(changedSlotValue).array(); 
				putChunk(ChunkHandle, bytesChangedSlotValue, ChunkServer.ChunkSize-4*i);
			}
			
		}
		
		//shift next available offset
		byte[] nextAvailableOffset = getChunk(ChunkHandle, 4, 4);
		int intNextAvailableOffset = ByteBuffer.wrap(nextAvailableOffset).getInt();
		intNextAvailableOffset -= shiftingFactor;
		ByteBuffer bIntNextAvailableOffset = ByteBuffer.allocate(4);
		byte[] newNextAvailableOffset = bIntNextAvailableOffset.putInt(intNextAvailableOffset).array();
		putChunk(ChunkHandle, newNextAvailableOffset, 4); 
		return true; 
	}
	
	public byte[] readFirstRecord(String ChunkHandle, RID rid) {
		byte[] numSlots = getChunk(ChunkHandle, 0, 4); 
		int intNumSlots = ByteBuffer.wrap(numSlots).getInt();
		if (intNumSlots == 0) {
			return null; 
		} else {
			byte[] offset = getChunk(ChunkHandle, ChunkServer.ChunkSize - 4, 4);
			int intOffset = ByteBuffer.wrap(offset).getInt(); 
			rid.setChunkHandle(ChunkHandle);
			rid.setSlotNumber(1);
			RID nextRID = new RID();
			if (intOffset == -1) {
				byte[] payload = readNextRecord(rid, nextRID);
				rid.setChunkHandle(nextRID.getChunkHandle());
				rid.setSlotNumber(nextRID.getSlotNumber());
				return payload; 
			}
			byte[] size = getChunk(ChunkHandle, intOffset, 4);
			int intSize = ByteBuffer.wrap(size).getInt(); 
			return getChunk(ChunkHandle, intOffset+4, intSize); 
		}
	}


	public byte[] readLastRecord(String ChunkHandle, RID rid) {
		byte[] numSlots = getChunk(ChunkHandle, 0, 4); 
		int intNumSlots = ByteBuffer.wrap(numSlots).getInt();
		if (intNumSlots == 0) {
			return null; 
		} else {
			byte[] offset = getChunk(ChunkHandle, ChunkServer.ChunkSize - 4*intNumSlots, 4);
			int intOffset = ByteBuffer.wrap(offset).getInt(); 
			rid.setChunkHandle(ChunkHandle);
			rid.setSlotNumber(intNumSlots);
			RID prevRID = new RID();
			if (intOffset == -1) {
				byte[] payload = readPrevRecord(rid, prevRID);
				rid.setChunkHandle(prevRID.getChunkHandle());
				rid.setSlotNumber(prevRID.getSlotNumber());
				return payload;
			}
			byte[] size = getChunk(ChunkHandle, intOffset, 4);
			int intSize = ByteBuffer.wrap(size).getInt(); 
			return getChunk(ChunkHandle, intOffset+4, intSize); 
		}
	}
	
	public byte[] readNextRecord(RID rid, RID nextRid) {
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
			if (intOffset == -1) {
				rid.setSlotNumber(nextSlotNumber);
				return readNextRecord(rid, nextRid);
			} else {
				nextRid.setChunkHandle(ChunkHandle);
				nextRid.setSlotNumber(nextSlotNumber);
			}
			byte[] size = getChunk(ChunkHandle, intOffset, 4);
			int intSize = ByteBuffer.wrap(size).getInt(); 
			return getChunk(ChunkHandle, intOffset+4, intSize); 
		}
	}
	
	public byte[] readPrevRecord(RID rid, RID prevRid) {
		String ChunkHandle = rid.getChunkHandle();
		int slotNumber = rid.getSlotNumber();
		if (slotNumber == 1) {
			return null;
		} else {
			int nextSlotNumber = slotNumber-1;
			byte[] offset = getChunk(ChunkHandle, ChunkServer.ChunkSize - 4*nextSlotNumber, 4);
			int intOffset = ByteBuffer.wrap(offset).getInt(); 
			if (intOffset == -1) {
				rid.setSlotNumber(nextSlotNumber);
				return readPrevRecord(rid, prevRid); 
			} else {
				prevRid.setChunkHandle(ChunkHandle);
				prevRid.setSlotNumber(nextSlotNumber);
			}
			byte[] size = getChunk(ChunkHandle, intOffset, 4);
			int intSize = ByteBuffer.wrap(size).getInt(); 
			return getChunk(ChunkHandle, intOffset+4, intSize); 
		}
	}

}