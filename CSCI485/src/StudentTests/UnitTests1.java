package StudentTests;

import java.util.Arrays;
import java.util.HashSet;

import com.client.ClientFS;
import com.client.ClientFS.FSReturnVals;

/**
 * Checks that multiple ClientFS can run with one master
 * @author Nandhini Namasivayam
 *
 */
public class UnitTests1 {
	public static int N = 100;
	static final String TestName = "Unit Test 1: ";
	
	public static void main(String[] args) {
		test1();
	}
	
	public static void test1(){
		// Create 2 ClientFSs
		ClientFS cfs = new ClientFS();
		ClientFS cfs2 = new ClientFS();
		System.out.println(TestName + "Create dir /MNV, /MNV/1, /MNV/2, /MNV/3, "
				+ "... /MNV/N from first client and verify them from second.");
		String dir1 = "MNV";
		
		// Create from first client (cfs)
		FSReturnVals fsrv = cfs.CreateDir("/", dir1);
		if ( fsrv != FSReturnVals.Success ){
			System.out.println("Unit test 1 result: fail!");
    		return;
		}
		String[] gen1 = new String[N];
		for(int i = 1; i <= N; i++){
			fsrv = cfs.CreateDir("/" + dir1 + "/", String.valueOf(i));
			if( fsrv != FSReturnVals.Success ){
				System.out.println("Unit test 1 result: fail!");
	    		return;
			}
			gen1[i - 1] = "/" + dir1 + "/" + i;
		}
		
		// Verify from second client (cfs2)
		String[] ret1 = cfs2.ListDir("/" + dir1);
		boolean compare1 = compareArrays(gen1, ret1);
		if(compare1 == false){
			System.out.println("Unit test 1 result: fail!");
    		return;
		}
		
		System.out.println(TestName + "Create dir /BKN, /BKN/1, /BKN/1/2, .... /BKN/1/2/.../N "
				+ "from second client and verify them from first.");
		
		// Create from second client (cfs2)
		String dir2 = "BKN";
		fsrv = cfs2.CreateDir("/", dir2);
		if( fsrv != FSReturnVals.Success ){
			System.out.println("Unit test 1 result: fail!");
    		return;
		}
		String[] gen2 = new String[N];
		String prev = "/" + dir2;
		for(int i = 1; i <= N; i++){
			fsrv = cfs2.CreateDir(prev + "/", String.valueOf(i));
			if( fsrv != FSReturnVals.Success ){
				System.out.println("Unit test 1 result: fail!");
	    		return;
			}
			prev = prev + "/" + i;
			gen2[i - 1] = prev;
		}	
		
		// Verify from first client
		ret1 = cfs.ListDir("/" + dir2);
		compare1 = compareArrays(gen2, ret1);
		if(compare1 == false){
			System.out.println("Unit test 1 result: fail!");
    		return;
		}
		
        System.out.println(TestName + "Success!"); 
	}
	
	public static boolean compareArrays(String[] arr1, String[] arr2) {
	    HashSet<String> set1 = new HashSet<String>(Arrays.asList(arr1));
	    HashSet<String> set2 = new HashSet<String>(Arrays.asList(arr2));
	    return set1.equals(set2);
	}
}
