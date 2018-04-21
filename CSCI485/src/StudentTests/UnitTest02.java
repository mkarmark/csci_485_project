package StudentTests;

import com.client.ClientFS;
import com.client.ClientFS.FSReturnVals;

/**
 * Checks that multiple clientFS can work with files on one master
 * @author Nandhini Namasivayam
 *
 */

public class UnitTest02 {
	public static int N = 755;
	static final String TestName = "Multi ClientFS Medium: ";

	public static void main(String[] args) {
		ClientFS cfs = new ClientFS();
		ClientFS cfs2 = new ClientFS();

		System.out.println(TestName + "CreateDir /MNV485 from first client");
		String dir1 = "MNV485";
		FSReturnVals fsrv = cfs.CreateDir("/", dir1);
		if( fsrv != FSReturnVals.Success ){
			System.out.println("Multi ClientFS Medium: fail!");
    		return;
		}
		
		System.out.println(TestName + "CreateFile Test1/2/.../15 in /MNV485 from second client");
		for(int i = 1; i <= N; i++){
			fsrv = cfs2.CreateFile("/" + dir1+"/", "Test" + i);
			if( fsrv != FSReturnVals.Success ){
				System.out.println("Unit test 3 result: fail!");
	    		return;
			}
		}
		
		System.out.println(TestName + "DeleteFile Test1/2/.../15 in /MNV485 from second client");
		for(int i = 1; i <= N; i++){
			fsrv = cfs2.DeleteFile("/" + dir1 + "/", "Test" + i);
			if( fsrv != FSReturnVals.Success ){
				System.out.println("Unit test 3 result: fail!");
	    		return;
			}
		}
		
		System.out.println(TestName + "Success!");
	}
}
