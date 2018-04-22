package StudentTests;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.client.ClientFS;
import com.client.ClientRec;
import com.client.FileHandle;
import com.client.RID;
import com.client.TinyRec;
import com.client.ClientFS.FSReturnVals;

public class UnitTest04 {
	public static int NumRecs = 1000;
	static final String TestName = "Logging Test Part 2: ";
	
	public static void main(String[] args) {
		String dir1 = "UT3";
		ClientFS cfs = new ClientFS();
		FSReturnVals fsrv;

		//get the file handle first
		FileHandle fh = new FileHandle();
		FSReturnVals ofd = cfs.OpenFile("/" + dir1 + "/test", fh);
		ClientRec crec = new ClientRec();
		
		System.out.println(TestName + "Scan all records in a file");
		ofd = cfs.OpenFile("/" + dir1 + "/test", fh);
		TinyRec r1 = new TinyRec();
		FSReturnVals retRR = crec.ReadFirstRecord(fh, r1);
		int cntr = 1;
		ArrayList<RID> vect = new ArrayList<RID>();
		while (r1.getRID() != null){
			TinyRec r2 = new TinyRec();
			FSReturnVals retval = crec.ReadNextRecord(fh, r1.getRID(), r2);
			if(r2.getRID() != null){
				byte[] head = new byte[4];
				System.arraycopy(r2.getPayload(), 0, head, 0, 4);
				int value = ((head[0] & 0xFF) << 24) | ((head[1] & 0xFF) << 16)
				        | ((head[2] & 0xFF) << 8) | (head[3] & 0xFF);
				
				//Store r2 in a vector
				if(value % 2 != 0){
					vect.add(r2.getRID());
				}
				r1 = r2;
				cntr++;
			} else {
				r1.setRID(null);
			}
				
		}
		
		System.out.println(TestName + "Delete the odd numbered records using their first four bytes.");
		//Iterate the vector and delete the RIDs stored in it
		for(int i = 0; i < vect.size(); i++){
			fsrv = crec.DeleteRecord(fh, vect.get(i));
			if(fsrv != FSReturnVals.Success){
				System.out.println("Logging Test Part 2 result: failed to delete the record!");
				return;
			}
		}
		
		fsrv = cfs.CloseFile(fh);
		if(cntr != NumRecs){
			System.out.println("Logging Test Part 2 result: fail!");
    		return;
		}
		
		System.out.println(TestName + "Scan the file and verify there are only even numbered records using their first four bytes.");
		ofd = cfs.OpenFile("/" + dir1 + "/test", fh);
		r1 = new TinyRec();
		retRR = crec.ReadFirstRecord(fh, r1);
		while (r1.getRID() != null){
			TinyRec r2 = new TinyRec();
			FSReturnVals retval = crec.ReadNextRecord(fh, r1.getRID(), r2);
			if(r2.getRID() != null){
				byte[] head = new byte[4];
				System.arraycopy(r2.getPayload(), 0, head, 0, 4);
				int value = ((head[0] & 0xFF) << 24) | ((head[1] & 0xFF) << 16)
				        | ((head[2] & 0xFF) << 8) | (head[3] & 0xFF);
				if(value % 2 != 0){
					System.out.println("Logging Test Part 2 result: fail!  Found an odd numbered record with value " + value + ".");
		    		return;
				}
				r1 = r2;
			}else{
				r1.setRID(null);
			}
		}
		fsrv = cfs.CloseFile(fh);
		System.out.println(TestName + "Success!");
	}
}
