import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class WorkerThread extends Thread {
	private String inprocessDir;
	private String fname;
	private Properties properties;
	
	{
		properties = new Properties();
		try {
			properties.load(new FileReader("src\\FileDetails.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public WorkerThread(String inprocessDir, String fname) {
		this.inprocessDir = inprocessDir;
		this.fname = fname;
	}

	@Override
	public void run() {
		System.out.println("WorkerThread");	
		DatabaseHandler db = new DatabaseHandler(fname.substring(0, fname.lastIndexOf(".")));
		
		String[][] fieldRecord = getfieldsDataTypes();
		db.createTables(fieldRecord);
		
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(inprocessDir+"\\"+fname));
			String line;			
			while((line = br.readLine()) != null) {
				String[] lineData = line.split(",");
				if(isValidFields(lineData)) {
					db.insertRecord(lineData, fieldRecord, "ValidRecord");
				}
				else {
					db.insertRecord(lineData, fieldRecord, "InValidRecord");
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	private String[][] getfieldsDataTypes() {
		String field = this.properties.get("DataTypes"+fname).toString();
		String[] fieldValues = field.split(",");
		String[][] fieldData = new String[fieldValues.length][2];
		for(int i=0 ; i<fieldValues.length ; i++) {
			String[] d = fieldValues[i].split("=");
			fieldData[i][0]=d[0];
			fieldData[i][1]=d[1];
		}
		return fieldData;
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
