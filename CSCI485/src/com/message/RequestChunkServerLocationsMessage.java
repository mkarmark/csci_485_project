package com.message;

import java.io.Serializable;
import java.util.Vector;

import com.chunkserver.Location;

public class RequestChunkServerLocationsMessage implements Serializable {
	private static final long serialVerisionUID = 1;
	
	private Vector<Location> locations;
	
	public RequestChunkServerLocationsMessage(Vector<Location> locations) {
		super();
		this.locations = new Vector<Location>();
		for(int i=0; i<locations.size(); i++) { 
			this.locations.add(locations.get(i));
		}
	}

	public RequestChunkServerLocationsMessage() {
		super();
	}

	public Vector<Location> getLocations() {
		return locations;
	}
}
