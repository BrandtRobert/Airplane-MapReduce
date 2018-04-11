package cs455.airlines;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AirlinesCombiner extends Reducer<Text, Text, Text, Text> {

	public Text combineValues(Text key, Iterable<Text> values) {
		String keyType = key.toString().split(":")[0];
		Text combinedVal = null;
		switch (keyType) {
		case "M1:":
		case "W1:":
		case "D1:":
		case "3:":
		case "6:":
			combinedVal = sumIterableValues(key, values);
			break;
		case "4:":
		case "5:":
			combinedVal = countValuesWithNumberEntries(key, values);
			break;
		default:
			combinedVal = new Text("Null Value : " + keyType);
		}
		return combinedVal;
	}

	/**
	 * Sums all the numerical values in the value list
	 * For our uses:
	 *   @param key - "<month-of-year> || <day-of-week> || <time-of-day> || <airport-code"
	 * @param value - An iterable containing int counts of values for this key
	 * @return a new text value that sums all the counts for this value list
	 */
	private Text sumIterableValues (Text key, Iterable<Text> value) {
		int sumDelays = 0;
		for (Text val : value) {
			String toStr = val.toString();
			sumDelays += Integer.parseInt(toStr);
		}
		return new Text("" + sumDelays);
	}

	/**
	 * Sums all the numerical values in the value list also includes number of entries
	 * For our uses:
	 *   @param key - "<plane-age> || <carrier->"
	 * @param value - An iterable containing int counts of values for this key
	 * @return a new text value that sums all the counts for this value list, also a count of all entries in the list
	 */
	private Text countValuesWithNumberEntries(Text key, Iterable<Text> value) {
		int sumDelays = 0;
		int countEntries = 0;
		for (Text val : value) {
			String toStr = val.toString();
			int valueParsed = Integer.parseInt(toStr);
			sumDelays += valueParsed;
			// Don't count 0 minutes delays
			if (valueParsed > 0) {
				countEntries++;
			}
		}
		return new Text(sumDelays + "," + countEntries);
	}

}
