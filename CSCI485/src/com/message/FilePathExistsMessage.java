package com.message;

import java.io.Serializable;

public class FilePathExistsMessage implements Serializable{
	private static final long serialVerisionUID = 1;
	
	private boolean exists;
	private String filePath;
	
	public FilePathExistsMessage(String filePath)
	{
		this.filePath = filePath;
	}
	
	public FilePathExistsMessage(boolean exists)
	{
		this.exists = exists;
	}

	public boolean isExists() {
		return exists;
	}

	public String getFilePath() {
		return filePath;
	}
	
	
}
