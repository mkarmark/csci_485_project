package com.message;

import java.io.Serializable;

import com.client.FileHandle;

public class OpenFileMessage implements Serializable{
	private static final long serialVerisionUID = 1;
	
	private String FilePath;
	private FileHandle fh;
	
	public OpenFileMessage(String filePath) {
		FilePath = filePath;
	}
	
	public OpenFileMessage(FileHandle fh) {
		this.fh = new FileHandle();
		
		this.fh.setChunks(fh.getChunks());
		this.fh.setFilepath(fh.getFilepath());
	}

	public String getFilePath() {
		return FilePath;
	}

	public FileHandle getFh() {
		return fh;
	}
	
}
