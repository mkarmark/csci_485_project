package com.client;

import java.io.Serializable;
import java.util.Vector;

public class FileHandle implements Serializable{
	private static final long serialVerisionUID = 1;
	private String filepath;
	private Vector<String> chunks;
	
	public FileHandle()
	{
		filepath = new String();
		chunks = new Vector<String>();
	}
	
	public FileHandle(String filepath, Vector<String> chunks) {
		this.filepath = filepath;
		this.chunks = chunks;
	}
	
	public String getFilepath() {
		return filepath;
	}
	
	public void setFilepath(String filepath) {
		this.filepath = filepath;
	}
	
	public Vector<String> getChunks() {
		return chunks;
	}
	
	public void setChunks(Vector<String> chunks) {
		this.chunks = chunks;
	}
	
	public void appendChunk(String chunk)
	{
		chunks.add(chunk);
	}
	
	public String getLastChunk()
	{
		return chunks.get(chunks.size()-1);
	}
	
	public String getFirstChunk()
	{
		return chunks.get(0);
	}
	
	public String getNextChunk(String chunk) {
		for (int i=0; i<chunks.size(); i++) {
			if (chunks.get(i).equals(chunk)) {
				if (i == chunks.size() - 1) {
					return null; 
				} else {
					return chunks.get(i+1);
				}
			}
		}
		return ""; 
	}
	
	public String getPrevChunk(String chunk) {
		for (int i=chunks.size() - 1; i>= 0; i--) {
			if (chunks.get(i).equals(chunk)) {
				if (i == 0) {
					return null;
				} else {
					return chunks.get(i-1);
				}
			}
		}
		return ""; 
	}
	
	
	/**File handle should have filepath and 
	 * a way to determine which chunk handles 
	 * correspond to this file.
	 */
}
