package com.client;

import java.util.Vector;

public class FileHandle {
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
	
	
	/**File handle should have filepath and 
	 * a way to determine which chunk handles 
	 * correspond to this file.
	 */
}
