package com.message;

import java.io.Serializable;
import java.util.Vector;

public class ListDirMessage implements Serializable{
	private static final long serialVerisionUID = 1;
	
	public String tgt;
	public Vector<String> results;
	
	// From ClientFS to Master
	public ListDirMessage(String tgt)
	{
		this.tgt = tgt;
		this.results = new Vector<String>();
	}
	
	// From Master to ClientFS
	public ListDirMessage(Vector<String> results)
	{
		this.results = new Vector<String>();
		for(int i=0; i<results.size(); i++)
		{
			this.results.add(results.get(i));
		}
	}

	public String getTgt() {
		return tgt;
	}

	public Vector<String> getResults() {
		return results;
	}
	
	
}
