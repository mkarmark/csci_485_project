package com.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Scanner;
import java.util.Vector;

import com.chunkserver.ChunkServer;
import com.master.Master;
import com.message.CreateDirMessage;
import com.message.CreateFileMessage;
import com.message.DeleteDirMessage;
import com.message.DeleteFileMessage;
import com.message.FilePathExistsMessage;
import com.message.ListDirMessage;
import com.message.OpenFileMessage;
import com.message.RenameDirMessage;

public class ClientFS {
	private ObjectInputStream ois;
	private ObjectOutputStream oos;

	public enum FSReturnVals {
		DirExists, // Returned by CreateDir when directory exists
		DirNotEmpty, //Returned when a non-empty directory is deleted
		SrcDirNotExistent, // Returned when source directory does not exist
		DestDirExists, // Returned when a destination directory exists
		FileExists, // Returned when a file exists
		FileDoesNotExist, // Returns when a file does not exist
		BadHandle, // Returned when the handle for an open file is not valid
		RecordTooLong, // Returned when a record size is larger than chunk size
		BadRecID, // The specified RID is not valid, used by DeleteRecord
		RecDoesNotExist, // The specified record does not exist, used by DeleteRecord
		NotImplemented, // Specific to CSCI 485 and its unit tests
		Success, //Returned when a method succeeds
		Fail //Returned when a method fails
	}
	
	// Constructor
	public ClientFS(){
		int port = 5858;
		
		// Get the port number
		File portFile = new File("MasterPort.txt");
		Scanner scanner;
		try {
			scanner = new Scanner(portFile);
			while(scanner.hasNext())
			{
				port = scanner.nextInt();
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			System.out.println("port file not found");
		}
		
		// Connect to the port
		try {
			System.out.println("Trying to connect to Master");
			// TODO: Get IP address of master from file
			Socket s = new Socket("localhost", port);
			
			ois = new ObjectInputStream(s.getInputStream());
			oos = new ObjectOutputStream(s.getOutputStream());
		} catch (IOException ioe) {
			System.out.println("ioe in clientFS constructor: " + ioe.getMessage());
		}
	}

	/**
	 * Creates the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: CreateDir("/", "Shahram"), CreateDir("/Shahram/",
	 * "CSCI485"), CreateDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals CreateDir(String src, String dirname) {
		CreateDirMessage cdm = new CreateDirMessage(src, dirname);
		
		try{
			// Send the message
			oos.writeObject(cdm);
			oos.flush();
			
			// Receive the response and cast
			Object o = null;
			o = ois.readObject();
			cdm = (CreateDirMessage)o;
			
			// Reset both streams
			oos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in clientFS: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In ClientFS createDir " + cnfe.getMessage());
		}
		
		// Get error from message
		int error = cdm.getError();
		
		// Return based on error
		switch(error)
		{
			case 0:
				return FSReturnVals.Success;
			case -1:
				return FSReturnVals.SrcDirNotExistent;
			case -2:
				return FSReturnVals.DirExists;
		}
		
		return null;
	}

	/**
	 * Deletes the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: DeleteDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals DeleteDir(String src, String dirname) {
		DeleteDirMessage ddm = new DeleteDirMessage(src, dirname);
		
		try{
			// Send the message
			oos.writeObject(ddm);
			oos.flush();
			
			// Receive the response and cast
			Object o = null;
			o = ois.readObject();
			ddm = (DeleteDirMessage)o;
			
			// Reset both streams
			oos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in clientFS: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In ClientFS createDir " + cnfe.getMessage());
		}
		
		// Get error from message
		int error = ddm.getError();
		
		switch(error)
		{
			case 0:
				return FSReturnVals.Success;
			case -1:
				return FSReturnVals.SrcDirNotExistent;
			case -2:
				return FSReturnVals.Fail;
			case -3:
				return FSReturnVals.DirNotEmpty;
		}
		
		return null;
	}

	/**
	 * Renames the specified src directory in the specified path to NewName
	 * Returns SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if a directory with NewName exists in the specified path
	 *
	 * Example usage: RenameDir("/Shahram/CSCI485", "/Shahram/CSCI550") changes
	 * "/Shahram/CSCI485" to "/Shahram/CSCI550"
	 */
	public FSReturnVals RenameDir(String src, String NewName) {
		RenameDirMessage rdm = new RenameDirMessage(src, NewName);
		
		try{
			// Send the message
			oos.writeObject(rdm);
			oos.flush();
			
			// Receive the response and cast
			Object o = null;
			o = ois.readObject();
			rdm = (RenameDirMessage)o;
			
			// Reset both streams
			oos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in clientFS: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In ClientFS createDir " + cnfe.getMessage());
		}
		
		int error = rdm.getError();
		
		switch(error)
		{
			case 0:
				return FSReturnVals.Success;
			case -1:
				return FSReturnVals.SrcDirNotExistent;
			case -2:
				return FSReturnVals.DestDirExists;
		}
		return null;
	}

	/**
	 * Lists the content of the target directory Returns SrcDirNotExistent if
	 * the target directory does not exist Returns null if the target directory
	 * is empty
	 *
	 * Example usage: ListDir("/Shahram/CSCI485")
	 */
	public String[] ListDir(String tgt) {
		ListDirMessage ldm = new ListDirMessage(tgt);
		
		try{
			// Send the message
			oos.writeObject(ldm);
			oos.flush();
			
			// Receive the response and cast
			Object o = null;
			o = ois.readObject();
			ldm = (ListDirMessage)o;
			
			// Reset both streams
			oos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in clientFS: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In ClientFS createDir " + cnfe.getMessage());
		}
		
		Vector<String> results = ldm.getResults();
		
		// If it's empty, return null
		if(results.isEmpty())
		{
			return null;
		}
		
		// If the tgt doesn't exist, return the error
		if(results.get(0).equals("DNE"))
		{
			// TODO: Return FSReturnVals.SrcDirNotExistent
			return (new String[0]);
		}
		
		// Turn the vector into a String[] to return
		String[] list = new String[results.size()];
		for(int i=0; i<results.size(); i++)
		{
			list[i] = results.get(i);
		}
		
		return list;
	}

	/**
	 * Creates the specified filename in the target directory Returns
	 * SrcDirNotExistent if the target directory does not exist Returns
	 * FileExists if the specified filename exists in the specified directory
	 *
	 * Example usage: Createfile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals CreateFile(String tgtdir, String filename) {
		CreateFileMessage cfm = new CreateFileMessage(tgtdir, filename);
		
		try{
			// Send the message
			oos.writeObject(cfm);
			oos.flush();
			
			// Receive the response and cast
			Object o = null;
			o = ois.readObject();
			cfm = (CreateFileMessage)o;
			
			// Reset both streams
			oos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in clientFS: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In ClientFS createDir " + cnfe.getMessage());
		}
		int error = cfm.getError();
		
		switch(error)
		{
			case 0:
				return FSReturnVals.Success;
			case -1:
				return FSReturnVals.SrcDirNotExistent;
			case -2:
				return FSReturnVals.FileExists;
		}
		return null;
	}

	/**
	 * Deletes the specified filename from the tgtdir Returns SrcDirNotExistent
	 * if the target directory does not exist Returns FileDoesNotExist if the
	 * specified filename is not-existent
	 *
	 * Example usage: DeleteFile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
		DeleteFileMessage dfm = new DeleteFileMessage(tgtdir, filename);
		
		try{
			// Send the message
			oos.writeObject(dfm);
			oos.flush();
			
			// Receive the response and cast
			Object o = null;
			o = ois.readObject();
			dfm = (DeleteFileMessage)o;
			
			// Reset both streams
			oos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in clientFS: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In ClientFS createDir " + cnfe.getMessage());
		}
		int error = dfm.getError();
		
		switch(error)
		{
			case 0:
				return FSReturnVals.Success;
			case -1:
				return FSReturnVals.SrcDirNotExistent;
			case -2:
				return FSReturnVals.FileDoesNotExist;
		}
		return null;
	}

	/**
	 * Opens the file specified by the FilePath and populates the FileHandle
	 * Returns FileDoesNotExist if the specified filename by FilePath is
	 * not-existent
	 *
	 * Example usage: OpenFile("/Shahram/CSCI485/Lecture1/Intro.pptx", FH1)
	 */
	public FSReturnVals OpenFile(String FilePath, FileHandle ofh) {
		// Return FileDoesNotExist if Filepath is not-existent (Need to check master)
		FilePathExistsMessage fpem = new FilePathExistsMessage(FilePath);
		
		try{
			// Send the message
			oos.writeObject(fpem);
			oos.flush();
			
			// Receive the response and cast
			Object o = null;
			o = ois.readObject();
			fpem = (FilePathExistsMessage)o;
			
			// Reset both streams
			oos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in clientFS: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In ClientFS createDir " + cnfe.getMessage());
		}
		
		if(!fpem.isExists())
		{
			return FSReturnVals.FileDoesNotExist;
		}
		
		// Get filehandle of file
		OpenFileMessage ofm = new OpenFileMessage(FilePath);

		try{
			// Send the message
			oos.writeObject(ofm);
			oos.flush();
			
			// Receive the response and cast
			Object o = null;
			o = ois.readObject();
			ofm = (OpenFileMessage)o;
			
			// Reset both streams
			oos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in clientFS: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In ClientFS createDir " + cnfe.getMessage());
		}
		
		
		// Deep copy results into given file handle
		ofh.setChunks(ofm.getFh().getChunks());
		ofh.setFilepath(ofm.getFh().getFilepath());
		
		return FSReturnVals.Success;
	}

	/**
	 * Closes the specified file handle Returns BadHandle if ofh is invalid
	 *
	 * Example usage: CloseFile(FH1)
	 */
	public FSReturnVals CloseFile(FileHandle ofh) {
		// Return FileDoesNotExist if Filepath is not-existent (Need to check master)
		FilePathExistsMessage fpem = new FilePathExistsMessage(ofh.getFilepath());
		
		try{
			// Send the message
			oos.writeObject(fpem);
			oos.flush();
			
			// Receive the response and cast
			Object o = null;
			o = ois.readObject();
			fpem = (FilePathExistsMessage)o;
			
			// Reset both streams
			oos.reset();
		} catch (IOException ioe) {
			System.out.println("ioe in clientFS: "+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("In ClientFS createDir " + cnfe.getMessage());
		}
		
		if(!fpem.isExists())
		{
			return FSReturnVals.BadHandle;
		}
		
		return FSReturnVals.Success;
	}

}
