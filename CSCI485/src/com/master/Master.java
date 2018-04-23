package com.master;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.InputMismatchException;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;

import com.chunkserver.ChunkServer;
import com.chunkserver.Location;
import com.client.FileHandle;

public class Master {
	public static String log = "logs\\log.txt";
	public static String logPath = "logs\\";
	public static HashSet<String> namespace; // Maintains all of the directories
	public static HashSet<FileHandle> filespace;
	
	private int numChunkServers = 0;
	
	private HashMap<Integer, Location> chunkServerLocations;
	private HashMap<Integer, Vector<String>> chunkServerChunks;
	private HashMap<String, Vector<String>> chunkIPToChunks;
	
	private int ticks = 0;
	
	private final ReentrantLock SLock;
	private final ReentrantLock XLock;
	
	/**
	 * Constructor
	 */
	public Master()
	{
		// Initialize locks
		SLock = new ReentrantLock();
		XLock = new ReentrantLock();
		
		// Create namespace
		namespace = new HashSet<String>();
		
		// Check if the namespace exists on disk
		CheckNamespace();
		
		// Create filespace
		filespace = new HashSet<FileHandle>();
		
		// Check if the filespace exists on disk
		CheckFilespace();
		
		chunkServerLocations = new HashMap<Integer, Location>();
		chunkServerChunks = new HashMap<Integer, Vector<String>>(); 
		chunkIPToChunks = new HashMap<String, Vector<String>>();
		
		// Check if chunkspace exists on disk
		CheckChunkspace();
		
		// Update from Log
		UpdateFromLog();
	}

/*** DIRECTORY CODE ***/
	
	/**Create directory
	 * return: an int representing the error status (0=success; -1=srcDNE; -2=DestExists)
	 */
	public int CreateDir(String src, String dirname)
	{
		// Exclusive lock to create directories
		XLock.lock();
		
		// Check if the source exists
		if(!namespace.contains(src))
		{
			XLock.unlock();
			// If not, return -1
			return -1;
		}
		
		// Check if the specified directory already exists
		if(namespace.contains(src+dirname+"/"))
		{
			XLock.unlock();
			return -2;
		}
		
		// Create the directory
		namespace.add(src+dirname+"/");
		
		// Write to Log
		BufferedWriter out = null;
		try {
			// The true at the end is so that we just add to the end of the log
			out = new BufferedWriter(new FileWriter(log,true));
			out.write("CreateDir::"+src+"::"+dirname);
			out.newLine();
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Unlock 
		XLock.unlock();
		
		// Increment ticks 
		Tick();
		
		return 0;
	}
	
	/**Delete Directory
	 * return int representing the error status (0=success; -1=srcDNE; -2=dirDNE; -3=dirNotEmpty) 
	 */
	public int DeleteDir(String src, String dirname)
	{
		// Exclusive lock to delete directories
		XLock.lock();
		
		// Check if the source exists
		if(!namespace.contains(src))
		{
			XLock.unlock();
			// If not, return -1
			return -1;
		}
		
		// Check if the specified directory exists
		if(!namespace.contains(src+dirname+"/"))
		{
			XLock.unlock();
			return -2;
		}
		
		int countSub = 0;
		// Traverse namespace and check if the directory has any subdirectories
		for(String dir : namespace)
		{
			if(dir.startsWith(src+dirname))
			{
				++countSub;
			}
		}
		
		// Count is 1 because of the directory itself so greater means subdirectories
		if(countSub>1)
		{
			XLock.unlock();
			return -3;
		}
		
		// No errors = delete it
		namespace.remove(src+dirname+"/");
		
		// Write to log
		BufferedWriter out = null;
		try {
			// The true at the end is so that we just add to the end of the log
			out = new BufferedWriter(new FileWriter(log,true));
			out.write("DeleteDir::"+src+"::"+dirname);
			out.newLine();
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Unlock
		XLock.unlock();
		
		// Increment ticks
		Tick();
		
		return 0;
	}
	
	/**Rename Directory
	 * @param src
	 * @param NewName
	 * @return int representing error code (0=success; -1=srcDNE; -2=NewNameExists) 
	 */
	public int RenameDir(String src, String NewName)
	{
		// Exclusive lock to delete directories
		XLock.lock();
				
		// Check if the source exists
		if(!namespace.contains(src+"/"))
		{
			XLock.unlock();
			// If not, return -1
			return -1;
		}
		
		// Check if the new name already exists
		if(namespace.contains(NewName+"/"))
		{
			XLock.unlock();
			return -2;
		}
		
		// Find all entries that contain old name
		Vector<String> toChange = new Vector<String>();
		for(String dir : namespace)
		{
			if(dir.startsWith(src+"/"))
			{
				toChange.add(dir);
			}
		}
		
		// Replace src with NewName
		int srcLength = src.length();
		for(String dir : toChange)
		{
			String newDir = NewName+dir.substring(srcLength,dir.length());
			namespace.add(newDir);
			namespace.remove(dir);
		}
		
		// Write to log
		BufferedWriter out = null;
		try {
			// The true at the end is so that we just add to the end of the log
			out = new BufferedWriter(new FileWriter(log,true));
			out.write("RenameDir::"+src+"::"+NewName);
			out.newLine();
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		XLock.unlock();
		
		// Increment Ticks
		Tick();
		
		return 0;
	}
	
	/**List Directories
	 * 
	 * @param tgt
	 * @return 
	 */
	public Vector<String> ListDir(String tgt)
	{
		// Lock with a read lock
		SLock.lock();
		
		Vector<String> results = new Vector<String>();
		
		// Check if the specified directory exists
		if(!namespace.contains(tgt+"/"))
		{
			results.add("DNE");
		}
		
		for(String dir : namespace)
		{
			if(dir.startsWith(tgt) && !dir.equals(tgt+"/"))
			{
				// Want to remove the last "/" when listing it
				if(!dir.equals("/") && dir.endsWith("/"))
				{
					dir = dir.substring(0, dir.length()-1);
				}
				results.add(dir);
			}
		}
		
		SLock.unlock();
		
		return results;
	}
	
	/**
	 * 
	 * @param tgtdir
	 * @param filename
	 * @return int representing error status (0=success; -1=srcDNE; -2=FileExists) 
	 */
	public int CreateFile(String tgtdir, String filename)
	{
		XLock.lock();
		
		// Check if the specified directory exists
		if(!namespace.contains(tgtdir))
		{
			XLock.unlock();
			return -1;
		}
		
		// Check if the filename already exists in that directory
		if(HasFilepath(tgtdir+filename))
		{
			XLock.unlock();
			return -2;
		}
		
		// Create new FileHandle and add it to HashSet
		String filepath = tgtdir+filename;
		FileHandle fh = new FileHandle(filepath, new Vector<String>());
		
		// Add filehandle to filespace
		filespace.add(fh);
		
		// Write to log
		BufferedWriter out = null;
		try {
			// The true at the end is so that we just add to the end of the log
			out = new BufferedWriter(new FileWriter(log,true));
			out.write("CreateFile::"+tgtdir+"::"+filename);
			out.newLine();
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		XLock.unlock();
		
		// Increment ticks
		Tick();
		
		// Success
		return 0;
	}
	
	/**
	 * 
	 * @param tgtdir
	 * @param filename
	 * @return int representing the error status (0=success; -1=srcDNE; -2=FileDNE)
	 */
	public int DeleteFile(String tgtdir, String filename)
	{
		XLock.lock();
		
		// Check if the specified directory exists
		if(!namespace.contains(tgtdir))
		{
			XLock.unlock();
			return -1;
		}
		
		// Check if the filename exists
		if(!HasFilepath(tgtdir+filename))
		{
			XLock.unlock();
			return -2;
		}
		
		// Remove filehandle
		String filepath = tgtdir+filename;
		FileHandle fh = new FileHandle(filepath, new Vector<String>());
		filespace.remove(fh);
		
		// Write to log
		BufferedWriter out = null;
		try {
			// The true at the end is so that we just add to the end of the log
			out = new BufferedWriter(new FileWriter(log,true));
			out.write("DeleteFile::"+tgtdir+"::"+filename);
			out.newLine();
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		XLock.unlock();
		
		// Increment Ticks
		Tick();
		
		// Success
		return 0;
	}
	
	/**
	 * 
	 * @param FilePath
	 * @return the filepath if exists, null otherwise
	 */
	public FileHandle OpenFile(String FilePath)
	{
		SLock.lock();
		
		for(FileHandle fh : filespace)
		{
			if(fh.getFilepath().equals(FilePath))
			{
				SLock.unlock();
				return fh;
			}
		}
		
		SLock.unlock();
		
		return null;
	}
	
	/**
	 * Check if the chosen filepath exists in the filespace hashset 
	 * @param filename
	 * @return
	 */
	public boolean HasFilepath(String filepath)
	{
		for(FileHandle fh : filespace)
		{
			if(fh.getFilepath().equals(filepath))
			{
				return true;
			}
		}
		
		return false;
	}
	
	/** Code to Log **/
	
	/**
	 * Check if a namespace.txt exists on disk
	 * Read in namespace if it does, create root if it doesn't
	 */
	public void CheckNamespace()
	{
		// Check if a namespace file exists and read all directories from it
		File nsp = new File(logPath+"namespace.txt");
		Scanner scanner;
		try {
			scanner = new Scanner(nsp);
			while(scanner.hasNext())
			{
				namespace.add(scanner.nextLine());
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			// If the file doesn't exist just add root directory
			namespace.add("/");
		}
	}
	
	/**
	 * Check if a filespace.txt exists on disk
	 * filespace is set up as a filepath followed by the number of 
	 * chunkhandles followed by the chunkhandles each on a new line
	 */
	public void CheckFilespace()
	{
		// Check if a namespace file exists and read all directories from it
		File fspc = new File(logPath+"filespace.txt");
		Scanner scanner;
		try {
			scanner = new Scanner(fspc);
			while(scanner.hasNext())
			{
				// Create a new FileHandle
				FileHandle newfh = new FileHandle();
				
				// Get the filepath
				newfh.setFilepath(scanner.nextLine());
				
				if(!scanner.hasNextLine())
				{
					break;
				}
				
				// Get the number of chunk handles
				int numChunks = scanner.nextInt();
				scanner.nextLine();
				
				// Get each chunk handle
				for(int i=0; i<numChunks; i++)
				{
					newfh.appendChunk(scanner.nextLine());
				}
				
				// Add the filehandle to the filespace
				filespace.add(newfh);
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			// If the file doesn't exist, do nothing
		}
	}
	
	/**
	 * Check if a chunkspace.txt exists on disk
	 * chunkspace is set up as a IP followed by the number of 
	 * chunkhandles followed by the chunkhandles each on a new line
	 */
	public void CheckChunkspace()
	{
		// Check if a chunkspace file exists and read all chunks from it
		File chunkspace = new File(logPath+"chunkspace.txt");
		Scanner scanner;
		try {
			scanner = new Scanner(chunkspace);
			while(scanner.hasNext())
			{
				// Get the IP address (as XXX.XXX.XXX.XXX)
				String ip = scanner.nextLine();
				
				if(!scanner.hasNextLine())
				{
					break;
				}
				
				// Get the number of chunk handles
				int numChunks = scanner.nextInt();
				scanner.nextLine();
				
				// Get each chunk handle
				Vector<String> chunkHandles = new Vector<String>();
				for(int i=0; i<numChunks; i++)
				{
					chunkHandles.add(scanner.nextLine());
				}
				
				// Add this information to chunkIPToChunks
				chunkIPToChunks.put(ip, chunkHandles);
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			// If the file doesn't exist, do nothing
		}
	}
	
	/**
	 * Write the namespace to a file by first creating a new file, 
	 * then deleting the old and renaming the new
	 */
	public void WriteNamespaceToDisk()
	{
		// Create a new namespace to not overwrite the old one
		File newNamespace = new File(logPath+"newnamespace.txt");
		
		// Write namespace out to this new file
		BufferedWriter out = null;
		try {
			out = new BufferedWriter(new FileWriter(logPath+"newnamespace.txt"));
			for(String dir : namespace)
			{
				out.write(dir);
				out.newLine();
				out.flush();
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Delete the old namespace and rename this new one
       File oldnamespace = new File(logPath+"namespace.txt");
       oldnamespace.delete();
       newNamespace.renameTo(oldnamespace);
	}
	
	/**
	 * Filespace is written as:
	 * Filepath (String)
	 * number of chunks (int)
	 * chunkhandle1 (String)
	 * ...
	 */
	public void WriteFilespaceToDisk()
	{
		// Create a new namespace to not overwrite the old one
		File newFilespace = new File(logPath+"newfilespace.txt");
		
		// Write namespace out to this new file
		BufferedWriter out = null;
		try {
			out = new BufferedWriter(new FileWriter(logPath+"newfilespace.txt"));
			for(FileHandle fh : filespace)
			{
				// First write filepath
				out.write(fh.getFilepath());
				out.newLine();
				
				// Get the chunks
				Vector<String> chunks = fh.getChunks();
				int chunkSize = chunks.size();
				
				// Write number of chunks
				out.write(""+chunkSize);
				out.newLine();
				
				// Write each of the chunks in order
				for(int i=0; i<chunkSize; i++)
				{
					out.write(chunks.get(i));
					out.newLine();
				}
				out.flush();
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Delete the old namespace and rename this new one
       File oldfilespace = new File(logPath+"filespace.txt");
       oldfilespace.delete();
       newFilespace.renameTo(oldfilespace);
	}
	
	/**
	 * Writes the relationship between ChunkServer and chunk to file as:
	 * ChunkServer IP (not port because that can change)
	 * number of chunks (int)
	 * chunkhandle1 (String)
	 */
	public void WriteChunkSpaceToDisk()
	{
		// Create a new chunkspace to not overwrite the old one
		File newChunkspace = new File(logPath+"newchunkspace.txt");
		
		// Write chunkspace out to this new file
		BufferedWriter out = null;
		try {
			out = new BufferedWriter(new FileWriter(logPath+"newchunkspace.txt"));
			
			// Loop through each ChunkServer
			Set<Integer> csIDs = chunkServerLocations.keySet();
			for(int csID : csIDs)
			{
				// Get the ChunkServer's location and write it to file
				Location loc = chunkServerLocations.get(csID);
				out.write(loc.getIp().toString());
				out.newLine();
				
				// Get the Vector of chunks
				Vector<String> chunks = chunkServerChunks.get(csID);
				int numChunks = chunks.size();
				
				// Write number of chunks
				out.write(""+numChunks);
				out.newLine();
				
				// Write each of the chunks in order
				for(int i=0; i<numChunks; i++)
				{
					out.write(chunks.get(i));
					out.newLine();
				}
				out.flush();
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Delete the old chunkspace and rename this new one
       File oldchunkspace = new File(logPath+"chunkspace.txt");
       oldchunkspace.delete();
       newChunkspace.renameTo(oldchunkspace);
	}
	
	/**
	 * Checkpoint and write to disk every 100 ticks
	 */
	public void Tick() {
		// Increment Ticks
		ticks++;
		
		if(ticks%10 != 0)
		{
			return;
		}
		
		// Write checkpoint to log
		BufferedWriter out = null;
		try {
			// The true at the end is so that we just add to the end of the log
			out = new BufferedWriter(new FileWriter(log,true));
			out.write("Checkpoint");
			out.newLine();
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		WriteNamespaceToDisk();
		WriteFilespaceToDisk();
		WriteChunkSpaceToDisk();
	}
	
	public void UpdateFromLog()
	{
		// Read log into array of strings representing commands
		File lg = new File(log);
		Scanner scanner;
		List<String> commands = new ArrayList<>();
		
		try {
			scanner = new Scanner(lg);
			while(scanner.hasNextLine())
			{
				String cmd = scanner.nextLine();
				
				// Clear the array list at a checkpoint
				if(cmd.equals("Checkpoint"))
				{
					commands.clear();
				} 
				else 
				{
					commands.add(cmd);
				}
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			// If the file doesn't exist, do nothing
		}
		
		// clear the log file
		BufferedWriter out = null;
		try {
			// The true at the end is so that we just add to the end of the log
			out = new BufferedWriter(new FileWriter(log));
			out.write("");
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Redo the list of commands
		int size = commands.size();
		for(int i=0; i<size; i++)
		{
			// Split the command by "::"
			String[] result = commands.get(i).split("::");
			String cmd = result[0];
			
			if(cmd.equals("CreateDir")) {
				CreateDir(result[1],result[2]);
			} 
			else if (cmd.equals("DeleteDir")) {
				DeleteDir(result[1], result[2]);
			} 
			else if (cmd.equals("RenameDir")){
				RenameDir(result[1], result[2]);
			} 
			else if (cmd.equals("CreateFile")) {
				CreateFile(result[1], result[2]);
			} 
			else if (cmd.equals("DeleteFile")) {
				DeleteFile(result[1], result[2]);
			} 
			else if (cmd.equals("chToCS")) {
				String ip = result[1];
				
				// Add the chunk to the corresponding ChunkServer IP
				if(chunkIPToChunks.containsKey(ip))
				{
					chunkIPToChunks.get(ip).add(result[2]);
				}
				else 
				{
					// Add the ChunkServer IP and the new vector with this chunk
					Vector<String> chunks = new Vector<String>();
					chunks.add(result[2]);
					
					chunkIPToChunks.put(ip, chunks);
				}
			} else if (cmd.equals("ChunkToFilespace")) {
				AddChunkToFilesSpace(result[1], result[2]);
			}
			else {
				// Do nothing
			}
		}
		
	}
	
	/** End of Logging Code **/
	
	public void AddChunkToFilesSpace(String filePath, String ChunkHandle) {
		FileHandle fh = OpenFile(filePath);
		fh.appendChunk(ChunkHandle);
		
		// Write to log
		BufferedWriter out = null;
		try {
			// The true at the end is so that we just add to the end of the log
			out = new BufferedWriter(new FileWriter(log,true));
			out.write("ChunkToFilespace::"+filePath+"::"+ChunkHandle);
			out.newLine();
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Tick();
	}

	public int AddChunkServer(Location location) {
		chunkServerLocations.put(numChunkServers, location);
		
		// Check if the location already exists in chunkIPTOChunks
		Vector<String> chunkHandles = chunkIPToChunks.get(location.getIp().toString());
		if(chunkHandles == null)
		{
			chunkHandles = new Vector<String>();
		}

		chunkServerChunks.put(numChunkServers, chunkHandles);
		System.out.println("Chunk Server " + numChunkServers + " at location " + location);
		
		numChunkServers++;
		
		return numChunkServers - 1;
	}
	
	public void AddChunkHandleToChunkServer(int chunkServer, String ChunkHandle) {
		chunkServerChunks.get(chunkServer).add(ChunkHandle);
		
		// Write to log
		BufferedWriter out = null;
		try {
			// The true at the end is so that we just add to the end of the log
			out = new BufferedWriter(new FileWriter(log,true));
			out.write("chToCS::"+chunkServerLocations.get(chunkServer).getIp().toString()+"::"+ChunkHandle);
			out.newLine();
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Tick();
	}
	
	public Vector<Location> GetChunkServerLocations() {
		Vector<Location> locations = new Vector<Location>();
		
		for (int i=0; i< numChunkServers; i++) {
			locations.add(chunkServerLocations.get(i));
		}
		
		return locations;
	}
	
	/** Main function **/
	public static void main(String [] args) {
		ServerSocket ss; 
		boolean havePort = false;
		int port = 5858;
		
		Master ms = new Master();
		
		// Try to connect to a port
		while(!havePort)
		{
			try
			{
				ss = new ServerSocket(port);
				System.out.println("Master's IP address is " + ss.getInetAddress().getLocalHost() + " and port is " + ss.getLocalPort()); 
				
				// Write IP and port out to file
				BufferedWriter writer;
				try {
					writer = new BufferedWriter(new FileWriter("MasterPort.txt"));
					String ip = ss.getInetAddress().getLocalHost().toString();
					writer.write("" + ip.substring(9)+ "\n");
					writer.write(""+port);
				    writer.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				
				// Loop to accept connections
				while(true) {
					Socket s = ss.accept();
					MasterThread mt = new MasterThread(s, ms);
					System.out.println("Master. Connection accepted from: "+s.getPort());
				}
			}
			catch (IOException ioe)
			{
				port++;
			}
		}
	}
}
