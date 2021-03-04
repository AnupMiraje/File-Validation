
public class ExhibitMonitor {

	public static void main(String[] args) {
		FileMonitor fm = new FileMonitor(MyConstants.INPUTDIR, MyConstants.INPROCESSDIR);
		fm.start();
	}
}
