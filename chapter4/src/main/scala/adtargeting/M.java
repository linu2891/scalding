package adtargeting;

import java.io.File;
import java.io.IOException;

public class M {
	
	public M(){
		
		main2();
	}
	
	public static void main(String[] args) {
		main2();
	}
	
	
	public static void main2() {
		System.out.println("java home :" + System.getenv("JAVA_HOME"));
		try {
			File f = new File("./");
			System.out.println("Current dir :" + f.getCanonicalPath());
		} catch (IOException e) {
			System.out.println("Current dir : err " + e);
			e.printStackTrace();
		}
		
	}

}
