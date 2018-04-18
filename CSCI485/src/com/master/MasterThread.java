package com.master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Vector;

import com.chunkserver.ChunkServer;
import com.client.FileHandle;
import com.message.CreateDirMessage;
import com.message.CreateFileMessage;
import com.message.DeleteDirMessage;
import com.message.DeleteFileMessage;
import com.message.FilePathExistsMessage;
import com.message.ListDirMessage;
import com.message.OpenFileMessage;
import com.message.RenameDirMessage;

public class MasterThread extends Thread{
	private ObjectOutputStream oos;
	private ObjectInputStream ois;
	private Master ms;
	
	public MasterThread(Socket s, Master ms) {
		try {
			this.ms = ms;
			oos = new ObjectOutputStream(s.getOutputStream());
			ois = new ObjectInputStream(s.getInputStream());
			this.start();
		} catch (IOException ioe) {
			System.out.println("ioe in MasterThread constructor: "+ioe.getMessage());
		}
	}
	
	public void run() {
		try {
			while(true) {
				Object o = ois.readObject();

				if(o instanceof CreateDirMessage)
				{
					// Cast incoming message
					CreateDirMessage cdm = (CreateDirMessage)o;
					
					// Get parameters
					String src = cdm.getSrc();
					String dirname = cdm.getDirname();
					
					// Get result from Master
					int error = ms.CreateDir(src, dirname);
					
					// Create outgoing message
					CreateDirMessage outCDM = new CreateDirMessage(error);
					
					// Write and flush
					oos.writeObject(outCDM);
				} else if(o instanceof ListDirMessage) {
					// Cast incoming message
					ListDirMessage ldm = (ListDirMessage)o;
					
					// Get parameters
					String tgt = ldm.getTgt();
					
					// Get result from Master
					Vector<String> results = ms.ListDir(tgt);
					
					// Create outgoing message
					ListDirMessage outLDM = new ListDirMessage(results);
					
					// Write and flush
					oos.writeObject(outLDM);	
				} else if(o instanceof DeleteDirMessage) {
					// Cast incoming message
					DeleteDirMessage ddm = (DeleteDirMessage)o;
					
					// Get parameters
					String src = ddm.getSrc();
					String dirname = ddm.getDirname();
					
					// Get result from Master
					int error = ms.DeleteDir(src, dirname);
					
					// Create outgoing message
					DeleteDirMessage outDDM = new DeleteDirMessage(error);
					
					// Write and flush
					oos.writeObject(outDDM);
				} else if(o instanceof RenameDirMessage) {
					// Cast incoming message
					RenameDirMessage rdm = (RenameDirMessage)o;
					
					// Get parameters
					String src = rdm.getSrc();
					String newName = rdm.getNewName();
					
					// Get result from Master
					int error = ms.RenameDir(src, newName);
					
					// Create outgoing message
					RenameDirMessage outRDM = new RenameDirMessage(error);
					
					// Write and flush
					oos.writeObject(outRDM);
				} else if(o instanceof CreateFileMessage) {
					// Cast incoming message
					CreateFileMessage cfm = (CreateFileMessage)o;
					
					// Get parameters
					String tgtdir = cfm.getTgtdir();
					String filename = cfm.getFilename();
					
					// Get result from Master
					int error = ms.CreateFile(tgtdir, filename);
					
					// Create outgoing message
					CreateFileMessage outCFM = new CreateFileMessage(error);
					
					// Write and flush
					oos.writeObject(outCFM);
				} else if(o instanceof DeleteFileMessage) {
					// Cast incoming message
					DeleteFileMessage dfm = (DeleteFileMessage)o;
					
					// Get parameters
					String tgtdir = dfm.getTgtdir();
					String filename = dfm.getFilename();
					
					// Get result from Master
					int error = ms.DeleteFile(tgtdir, filename);
					
					// Create outgoing message
					DeleteFileMessage outDFM = new DeleteFileMessage(error);
					
					// Write and flush
					oos.writeObject(outDFM);
				} else if(o instanceof FilePathExistsMessage) {
					// Cast incoming message
					FilePathExistsMessage fpem = (FilePathExistsMessage)o;
					
					// Get parameters
					String filePath = fpem.getFilePath();
					
					// Get result from Master
					boolean exists = ms.HasFilepath(filePath);
					
					// Create outgoing message
					FilePathExistsMessage outFPEM = new FilePathExistsMessage(exists);
					
					// Write and flush
					oos.writeObject(outFPEM);
				} else if(o instanceof OpenFileMessage) {
					// Cast incoming message
					OpenFileMessage ofm = (OpenFileMessage)o;
					
					// Get parameters
					String filePath = ofm.getFilePath();
					
					// Get result from Master
					FileHandle fh = ms.OpenFile(filePath);
					
					// Create outgoing message
					OpenFileMessage outOFM = new OpenFileMessage(fh);
					
					// Write and flush
					oos.writeObject(outOFM);
				}
				oos.flush();
				oos.reset();
			}
		} catch (IOException ioe) {
			System.out.println("mtRun ioe:"+ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("mtRun cnfe:"+cnfe.getMessage());
		}
	}
	
	// TODO: Complete this
}
