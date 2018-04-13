package com.master;

import java.util.HashSet;
import java.util.Vector;

public class Master {
	public static HashSet<String> namespace; // Maintains all of the directories
	
	/**
	 * Constructor
	 */
	public Master()
	{
		// Create namespace and add root directory
		namespace = new HashSet<String>();
		namespace.add("/"); //TODO: Change for Windows
		
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
		
		return 0;
	}
	
	/**
	 * 
	 * @param src
	 * @param NewName
	 * @return int representing error code (0=success; -1=srcDNE; -2=NewNameExists 
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

}
