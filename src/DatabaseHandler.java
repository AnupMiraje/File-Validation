import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseHandler {
	private String fname;
	private int noOfAddColumns=0;
	
	public DatabaseHandler(String fname) {
		this.fname = fname;
	}

	private static Connection con = null;
	private static Statement st = null;
	private PreparedStatement ps;

	static {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe", "RKIT", "RKIT");
			st = con.createStatement();
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
	}

	public void createTables(String[][] fieldData) {
		String fields = ""  + fieldData[0][0] + " " + fieldData[0][1];
		for(int i=1 ; i<fieldData.length ; i++) {
			fields = fields + "," + fieldData[i][0] + " " + fieldData[i][1];
		}
		try {
			String query = "CREATE TABLE ValidRecord"+fname+"(" + fields + ")";
			st.executeUpdate(query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		try {
			String query = "CREATE TABLE InValidRecord"+fname+"(" + fields + ")";
			st.executeUpdate(query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void insertRecord(String[] linedata, String[][] fieldRecord, String tableStatus) {
		
		//Add columns in table for invalidRecords
		for(int i=noOfAddColumns ; i<(linedata.length-fieldRecord.length) ; i++) {
			try {
				String alterQuery = "alter table " + tableStatus + fname + " add ExtraColumn" + (++noOfAddColumns) + " varchar(255)";
				st.executeUpdate(alterQuery);
			}
			catch(SQLException se) {
				se.printStackTrace();
			}
		}
		try {
			int tableColumns = fieldRecord.length;
			if(tableStatus.equals("InValidRecord"))
				tableColumns += noOfAddColumns;
			
			String s = "?";
			for(int i=1 ; i<tableColumns ; i++) {
				s = s + ",?";
			}
			String sql="INSERT INTO " + tableStatus + fname + " values("+s+")";
	        ps = con.prepareStatement(sql);
	        for(int i=0 ; i<tableColumns ; i++) {
	        	try {
	        		ps.setString(i+1, linedata[i]);
	        	}
	        	catch(Exception ae) {
	        		ps.setString(i+1, null);
	        	}
	        }
	        ps.executeUpdate();
		} 
		catch(SQLException se) {
			se.printStackTrace();
		}
	}
}
