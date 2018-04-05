package cs455.hadoop.mapreduce.delays;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DelaysReducer extends Reducer<Text, Text, Text, Text> {
	private final CarrierDelayTracker carrierTracker = new CarrierDelayTracker();
    // Year-0, Month-1, DofWeek-2, DepAct-3, DepSchd-4, ArrAct-5, ArrSchd-6,
    // CarrCode-7, CarrName-8, Tail#-9, ManuYear-10, City-11, Dest-12, CarrDel-13, 
    // WeaDel-14, NASDel-15, SecDel-16, LatDel-17
	public void reduce (Text airportCode, Iterable<Text> flightRecords, Context context) {
		for (Text record : flightRecords) {
			 String [] splits = record.toString().split(",");
			 int minutesDelayed;
			 try {
				// Prefer using the carrier delay field, if not available calculate arrival delay
				if (!splits[13].equals("NA")) {
					minutesDelayed = Integer.parseInt(splits[13]);
				} else {
					minutesDelayed = Integer.parseInt(splits[5]) - Integer.parseInt(splits[6]);
					// If the plane arrived before scheluded count it as 0 delay
					minutesDelayed = Math.max(minutesDelayed, 0);
				}
			 } catch (NumberFormatException e) {
				 Text report = new Text( splits[5] + " " + splits[6] + " " + splits[13] );
				 try {
					context.write(new Text("Fields"), report);
					} catch (IOException e1) {
						e1.printStackTrace();
					} catch (InterruptedException e1) {
						e1.printStackTrace();
				 }
				 minutesDelayed = 0;
			 }
			 carrierTracker.addDelay(splits[8], minutesDelayed);
		}
	}
	
	public void cleanup(Context context) {
		try {
			Map<String, String> delayStats = carrierTracker.getDelayStats();
			for (Entry<String, String> entry : delayStats.entrySet()) {
				context.write(new Text(entry.getKey()), new Text (entry.getValue()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
