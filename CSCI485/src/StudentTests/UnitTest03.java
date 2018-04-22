package StudentTests;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.client.ClientFS;
import com.client.ClientRec;
import com.client.FileHandle;
import com.client.RID;
import com.client.TinyRec;
import com.client.ClientFS.FSReturnVals;

public class UnitTest03 {
	public static int NumRecs = 1000;
	static final String TestName = "Logging Test Part 1: ";
	
	public static void main(String[] args) {
		System.out.println("Logging Test Part 1 simply creates files");
		String dir1 = "UT3";
		ClientFS cfs = new ClientFS();
		FSReturnVals fsrv = cfs.CreateDir("/", dir1);
		if ( fsrv != FSReturnVals.Success ){
			System.out.println("Logging Test Part 1: fail!");
    		return;
		}
		fsrv = cfs.CreateFile("/" + dir1 + "/", "test");
		if( fsrv != FSReturnVals.Success ){
			System.out.println("Logging Test Part 1 result: fail!");
    		return;
		}
		//get the file handle first
		FileHandle fh = new FileHandle();
		FSReturnVals ofd = cfs.OpenFile("/" + dir1 + "/test", fh);
		byte[] payload = null;
		int intSize = Integer.SIZE / Byte.SIZE;	// 4 bytes
		ClientRec crec = new ClientRec();
		
		System.out.println(TestName + "Construct a record with the first four bytes equal to i, followed with 5 char attributes each with length 20.");
		for (int i = 0; i < NumRecs; i++){
			payload = new byte[104];
			byte[] ValInBytes = ByteBuffer.allocate(intSize).putInt(i).array();
			System.arraycopy(ValInBytes, 0, payload, 0, intSize);
			for(int j = 4; j < 104; j++){
				payload[j] = 'a';
			}
			RID rid = new RID();
			crec.AppendRecord(fh, payload, rid);
		}
		fsrv = cfs.CloseFile(fh);
		
		System.out.println(TestName + "Success!");
	}
}
