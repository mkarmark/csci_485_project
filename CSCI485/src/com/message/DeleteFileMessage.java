package com.message;

import java.io.Serializable;

public class DeleteFileMessage implements Serializable{
	private static final long serialVerisionUID = 1;
	
	// The two parameters of CreateDir
	private String tgtdir;
	private String filename;
	private int error;
	
	// Constructor from Master to ClientFS
	public DeleteFileMessage(int error)
	{
		this.error = error;
	}
	
	// Constructor from ClientFS to Master
	public DeleteFileMessage(String tgtdir, String filename)
	{
		this.tgtdir = tgtdir;
		this.filename = filename;
	}
	
	/** Getters **/
	public String getTgtdir() {
		return tgtdir;
	}

	public String getFilename() {
		return filename;
	}
	
	public int getError() {
		return error;
	}
}
