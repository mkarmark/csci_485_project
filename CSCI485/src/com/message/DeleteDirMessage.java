package com.message;

import java.io.Serializable;

public class DeleteDirMessage implements Serializable{
	private static final long serialVerisionUID = 1;
	
	// The two parameters of CreateDir
	private String src;
	private String dirname;
	private int error;
	
	// Constructor from Master to ClientFS
	public DeleteDirMessage(int error)
	{
		this.error = error;
	}
	
	// Constructor from ClientFS to Master
	public DeleteDirMessage(String src, String dirname)
	{
		this.src = src;
		this.dirname = dirname;
	}
	
	/** Getters **/
	public String getSrc() {
		return src;
	}

	public String getDirname() {
		return dirname;
	}
	
	public int getError() {
		return error;
	}
}
