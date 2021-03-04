import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

public class WorkerThreadWithFileHandling extends Thread {
	private String inprocessDir;
	private String fname;
	private Properties properties;
	private BufferedWriter bufferedvalid = null;
	private BufferedWriter bufferedinvalid = null;
	
	{
		properties = new Properties();
		try {
			properties.load(new FileReader("C:\\Users\\Anup\\eclipse-workspace\\Validation\\src\\FileDetails.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public WorkerThreadWithFileHandling(String inprocessDir, String fname) {
		this.inprocessDir = inprocessDir;
		this.fname = fname;
	}

	@Override
	public void run() {
		System.out.println("WorkerThread");	
		//Using FileHandling
		try {
			bufferedvalid = new BufferedWriter(new FileWriter(inprocessDir+"\\ValidRecord"+fname));
			bufferedinvalid = new BufferedWriter(new FileWriter(inprocessDir+"\\InvalidRecord"+fname));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(inprocessDir+"\\"+fname));
			String line;
			while((line = br.readLine()) != null) {
				String[] lineData = line.split(",");
				if(isValidFields(lineData)) {
					bufferedvalid.write(line);
					bufferedvalid.newLine();
				}
				else {
					bufferedinvalid.write(line);
					bufferedinvalid.newLine();
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		finally {
			try {
				bufferedvalid.flush();
				bufferedinvalid.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private boolean isValidFields(String[] lineData) {
		String field = this.properties.get(fname).toString();
		String[] fieldValues = field.split(",");
		if(fieldValues.length != lineData.length)
			return false;
		for(int i=0 ; i<lineData.length ; i++) {
			if(Integer.parseInt(fieldValues[i])!=lineData[i].length()) {
				return false;
			}
		}
		return true;
	}
	
}
