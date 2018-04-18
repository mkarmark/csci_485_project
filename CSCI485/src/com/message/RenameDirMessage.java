package com.message;

import java.io.Serializable;

public class RenameDirMessage implements Serializable{
	private static final long serialVerisionUID = 1;
	
	// The two parameters of CreateDir
	private String src;
	private String newName;
	private int error;
	
	// Constructor from Master to ClientFS
	public RenameDirMessage(int error)
	{
		this.error = error;
	}
	
	// Constructor from ClientFS to Master
	public RenameDirMessage(String src, String newName)
	{
		this.src = src;
		this.newName = newName;
	}
	
	/** Getters **/
	public String getSrc() {
		return src;
	}

	public String getNewName() {
		return newName;
	}
	
	public int getError() {
		return error;
	}
}
