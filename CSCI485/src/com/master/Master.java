package com.master;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Vector;

import com.chunkserver.ChunkServer;
import com.client.FileHandle;

public class Master {
	public static String log = "log.txt";
	public static HashSet<String> namespace; // Maintains all of the directories
	public static HashSet<FileHandle> filespace;
	public static ChunkServer cs = new ChunkServer();
	
	/**
	 * Constructor
	 */
	public Master()
	{
		// Create namespace
		namespace = new HashSet<String>();
		
		// Check if the namespace exists on disk
		CheckNamespace();
		
		// Create filespace
		filespace = new HashSet<FileHandle>();
		
		// Check if the filespace exists on disk
		CheckFilespace();
		
		// TODO: Will have to connect to network like chunkserver
	}
	
	/**Create directory
	 * 
	 * return: an int representing the error status (0=success; -1=srcDNE; -2=DestExists)
	 */
	public int CreateDir(String src, String dirname)
	{
		// Check if the source exists
		if(!namespace.contains(src))
		{
			// If not, return -1
			return -1;
		}
		
		// Check if the specified directory already exists
		if(namespace.contains(src+dirname+"/"))
		{
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
		
		return 0;
	}
	
	/**Delete Directory
	 * return int representing the error status (0=success; -1=srcDNE; -2=dirDNE; -3=dirNotEmpty) 
	 */
	public int DeleteDir(String src, String dirname)
	{
		// Check if the source exists
		if(!namespace.contains(src))
		{
			// If not, return -1
			return -1;
		}
		
		// Check if the specified directory exists
		if(!namespace.contains(src+dirname+"/"))
		{
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
			return -3;
		}
		
		// No errors = delete it
		namespace.remove(src+dirname+"/");
		
		// TODO: Write to log
		
		return 0;
	}
	
	/**
	 * 
	 * @param src
	 * @param NewName
	 * @return int representing error code (0=success; -1=srcDNE; -2=NewNameExists) 
	 */
	public int RenameDir(String src, String NewName)
	{
		// Check if the source exists
		if(!namespace.contains(src+"/"))
		{
			// If not, return -1
			return -1;
		}
		
		// Check if the new name already exists
		if(namespace.contains(NewName+"/"))
		{
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
		
		// TODO: Write to log
		
		return 0;
	}
	
	/**List Directories
	 * 
	 * @param tgt
	 * @return 
	 */
	public Vector<String> ListDir(String tgt)
	{
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
		// Check if the specified directory exists
		if(!namespace.contains(tgtdir))
		{
			return -1;
		}
		
		// Check if the filename already exists in that directory
		if(HasFilepath(tgtdir+filename))
		{
			return -2;
		}
		
		// Create new FileHandle and add it to HashSet
		String filepath = tgtdir+filename;
		FileHandle fh = new FileHandle(filepath, new Vector<String>());
		
		// Add filehandle to filespace
		filespace.add(fh);
		
		// TODO: Write to log
		
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
		// Check if the specified directory exists
		if(!namespace.contains(tgtdir))
		{
			return -1;
		}
		
		// Check if the filename exists
		if(!HasFilepath(tgtdir+filename))
		{
			return -2;
		}
		
		// Remove filehandle
		String filepath = tgtdir+filename;
		FileHandle fh = new FileHandle(filepath, new Vector<String>());
		filespace.remove(fh);
		
		// TODO: Write to log
		
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
		for(FileHandle fh : filespace)
		{
			if(fh.getFilepath().equals(FilePath))
			{
				return fh;
			}
		}
		
		// TODO: Write to log
		
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
	
	/** Code to write namespace to file **/
	
	/**
	 * Check if a namespace.txt exists on disk
	 * Read in namespace if it does, create root if it doesn't
	 */
	public void CheckNamespace()
	{
		// Check if a namespace file exists and read all directories from it
		File nsp = new File("namespace.txt");
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
		File nfp = new File("filespace.txt");
		Scanner scanner;
		try {
			scanner = new Scanner(nfp);
			while(scanner.hasNext())
			{
				// Create a new FileHandle
				FileHandle newfh = new FileHandle();
				
				// Get the filepath
				newfh.setFilepath(scanner.nextLine());
				
				// Get the number of chunk handles
				int numChunks = scanner.nextInt();
				
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
	 * Write the namespace to a file by first creating a new file, 
	 * then deleting the old and renaming the new
	 */
	public void WriteNamespaceToDisk()
	{
		// Create a new namespace to not overwrite the old one
		File newNamespace = new File("newnamespace.txt");
		
		// Write namespace out to this new file
		BufferedWriter out = null;
		try {
			out = new BufferedWriter(new FileWriter("newnamespace.txt"));
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
       File oldnamespace = new File("namespace.txt");
       oldnamespace.delete();
       newNamespace.renameTo(oldnamespace);
       
       // TODO: Clear the log
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
		File newFilespace = new File("newfilespace.txt");
		
		// Write namespace out to this new file
		BufferedWriter out = null;
		try {
			out = new BufferedWriter(new FileWriter("newfilespace.txt"));
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
       File oldfilespace = new File("filespace.txt");
       oldfilespace.delete();
       newFilespace.renameTo(oldfilespace);
       
       // TODO: Clear the log
	}

}
