package hikst.crawler;

/**
 * AliveMessenger
 * Reports to the database every minute that it is alive and running.
 * Make sure settings are correctly set up before using.
 * Make sure this class gets loaded by the main tread.
 * The use of timer seem to take care of the rest.
 * 
 * Example:
 * public static void main(String[] args) {
 * 	new Settings();
 * 	AliveMessenger.getInstance();
 * }
 */

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AliveMessenger implements Runnable {
	private static AliveMessenger _instance = new AliveMessenger();;
	private final int INTERVAL = 60000;
	private int status_id;
	private long lastTimeRun;
	private static Thread messenger;
	

	private AliveMessenger() {
		// Status can not be null. Assuming this is set before any Collectors has started, therefor load = low
		try {
			status_id = Status.getInstance().getStatusID(Collector.Collector_Work_Status_Low);
		} catch (StatusIdNotFoundException e) {
			e.printStackTrace();
			// I am a vengeful bitch
			System.exit(0);
		}
		
		messenger = new Thread(this);
		messenger.start();
	}
	
	// Returning the singleton
    public static AliveMessenger getInstance() {
        return _instance;
    }

	// Returns the current external ip determined by a 3rd party.
	private String getIp() {
		try {
			URL url = new URL("http://ip.goldclone.no/");
			BufferedReader in = new BufferedReader(new InputStreamReader(
					url.openStream()));
			return in.readLine();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private void update(){
		String query = "";
		if (Settings.getCollectorID() == -1){
			// Insert
			query = "INSERT INTO Crawler(" +
					"status_id, " +
					"last_seen_ts ) " +
					"VALUES(?,extract(epoch from now())) RETURNING *";
			try {
				PreparedStatement statement = Settings.getDBC().prepareStatement(query);
				statement.setInt(1, status_id);
				ResultSet res = statement.executeQuery();
				res.next();
				Settings.setCollectorID(res.getInt("id"));
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}else {
			// Update
			query = "UPDATE crawler SET " +
					"status_id = ?, " +
					"last_seen_ts = extract(epoch from now()) " +
					"WHERE id = ?";
			try {
				PreparedStatement statement = Settings.getDBC().prepareStatement(query);
				statement.setInt(1, status_id);
				statement.setInt(2, Settings.getCollectorID());
				statement.executeUpdate();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void setStatus(String status) throws StatusIdNotFoundException{
		status_id = Status.getInstance().getStatusID(status);
	}

	@Override
	public void run() {
		while(true){
			lastTimeRun = System.currentTimeMillis();
			update();
//			System.out.println("Update took " + (System.currentTimeMillis() - lastTimeRun) + "ms");
			sleep(INTERVAL - (System.currentTimeMillis() - lastTimeRun));
		}
	}

	private void sleep(long l) {
		try {
			Thread.sleep(l);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
