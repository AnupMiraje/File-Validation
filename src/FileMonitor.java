import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/*
 * Single Responsibility Principle(SRP)
 */

public class FileMonitor extends Thread {
	private String inputDir;
	private String inprocessDir;
	private Properties properties;
	private Map<String,Calendar> fileArrivalRecord = new HashMap<String, Calendar>();
	
	{
		properties = new Properties();
		try {
			properties.load(new FileReader("src\\ValidFiles.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		fileArrivalRecord = recoverArrivalRecord();
	}
	
	public FileMonitor(String path, String inprocessFoler){
		this.inputDir = path;
		this.inprocessDir = inprocessFoler;
	}
	
	private Map<String, Calendar> recoverArrivalRecord() {
		Map<String,Calendar> recordMap = new HashMap<String, Calendar>();
		FileInputStream fis=null;
		ObjectInputStream ois=null;
		try {
			fis = new FileInputStream("src\\arrival.ser");
			ois = new ObjectInputStream(fis);
			recordMap= (Map)ois.readObject();
		} 
		catch (FileNotFoundException e) {
			File file = new File("src\\arrival.ser");
			try {
				file.createNewFile();
			}
			catch (IOException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		} 
		catch(EOFException eof) {
			
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return recordMap;		
	}
	
	//Single Responsibility Principle(SRP)
	public void run() {	
		File dir = new File(this.inputDir);
		while(true) {
			File[] files = dir.listFiles();
			for(File f : files) {
				if(isValid(f) && isInTime(f) && !isDuplicate(f)) {
					saveOrUpdateArrivalRecord(f.getName(),new GregorianCalendar());
					try {
						Files.move(Paths.get(inputDir+"\\"+f.getName()), Paths.get(inprocessDir+"\\"+f.getName()));
						WorkerThread worker = new WorkerThread(inprocessDir, f.getName());
						worker.start();
					} 
					catch (IOException e) {
						e.printStackTrace();
					}
				}
				else {
					f.delete();
				}
			}
			try {
				Thread.sleep(5000);
			} 
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void saveOrUpdateArrivalRecord(String name, GregorianCalendar calendar) {
		this.fileArrivalRecord.put(name, calendar);
		serializeMap(this.fileArrivalRecord);
	}
	
	private void serializeMap(Map<String, Calendar> fileArrivalRecord2) {
		FileOutputStream fos=null;
		ObjectOutputStream oos=null;
		try {
			fos = new FileOutputStream("src\\arrival.ser");
			oos = new ObjectOutputStream(fos);
			oos.writeObject(fileArrivalRecord2);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			try {
				oos.close();
				fos.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private boolean isDuplicate(File f) {
		if(fileArrivalRecord.containsKey(f.getName())) {
			Calendar cal = fileArrivalRecord.get(f.getName());
			int earlierArrivalDay = cal.get(Calendar.DAY_OF_MONTH);
			int today = new GregorianCalendar().get(Calendar.DAY_OF_MONTH);
			return earlierArrivalDay==today;
		}
		return false;
	}
	
	private boolean isInTime(File f) {
		return f.lastModified() < getExpectedTime(f.getName());
	}	
	
	private boolean isValid(File f) {
		return this.properties.containsKey(f.getName());
	}
	
	private long getExpectedTime(String fname) {
		String expectedTimeString = this.properties.getProperty(fname);
		int hour = Integer.parseInt(expectedTimeString.split(":")[0]);
		int min = Integer.parseInt(expectedTimeString.split(":")[1]);
		int second = Integer.parseInt(expectedTimeString.split(":")[2]);
		Calendar cal = new GregorianCalendar();
		cal.set(Calendar.HOUR_OF_DAY, hour);
		cal.set(Calendar.MINUTE, min);
		cal.set(Calendar.SECOND, second);
		return cal.getTimeInMillis();
	}
	
}
