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
			 	minutesDelayed = Integer.parseInt(splits[12]);
			 } catch (NumberFormatException e) {
			 	minutesDelayed = 0;
			 }
			 carrierTracker.addDelay(splits[8], minutesDelayed);
			 try {
				context.write(new Text("Record"), record);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void cleanup(Context context) {
		try {
			Map<String, String> delayStats = carrierTracker.getDelayStats();
			context.write(new Text("DelayStats.size"), new Text(delayStats.size() + ""));
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
