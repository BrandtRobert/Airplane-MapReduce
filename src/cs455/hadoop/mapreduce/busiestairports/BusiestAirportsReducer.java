package cs455.hadoop.mapreduce.busiestairports;

import java.util.TreeMap;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusiestAirportsReducer extends Reducer<Text, Text, Text, IntWritable> {

	private final Map<CityYearCombo, MutableInt> cityYearCounts = new TreeMap<CityYearCombo, MutableInt>();

	// Credit to: gregory
	// https://stackoverflow.com/questions/81346/most-efficient-way-to-increment-a-map-value-in-java
	private class MutableInt {
		private int value = 1;
		void increment() {
			value++;
		}
		int get() {
			return value;
		}
	}

	private class CityYearCombo {
		final String city;
		final int year;

		public CityYearCombo(String c, String y) {
			city = c;
			year = Integer.parseInt(y);
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((city == null) ? 0 : city.hashCode());
			result = prime * result + year;
			return result;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			CityYearCombo other = (CityYearCombo) obj;
			if (!getOuterType().equals(other.getOuterType())) {
				return false;
			}
			if (city == null) {
				if (other.city != null) {
					return false;
				}
			} else if (!city.equals(other.city)) {
				return false;
			}
			if (year != other.year) {
				return false;
			}
			return true;
		}

		private BusiestAirportsReducer getOuterType() {
			return BusiestAirportsReducer.this;
		}

	}

	private class CityYearFlights implements Comparable<CityYearFlights> {
		final int flights;
		final String city;
		final int year;

		public CityYearFlights(int flights, String city, int year) {
			this.flights = flights;
			this.city = city;
			this.year = year;
		}

		@Override
		public int compareTo(CityYearFlights other) {
			if (this.year == other.year) {
				return this.flights - other.flights;
			} else {
				return this.year - other.year;
			}
		}

	}

	public void reduce(Text city, Iterable<Text> years, Context context) {
		// Count up the values for every city year combination
		int count = 0;
		for (Text year : years) {
			CityYearCombo mapKey = new CityYearCombo(city.toString(), year.toString());
			if (!cityYearCounts.containsKey(mapKey)) {
				cityYearCounts.put(mapKey, new MutableInt());
			} else {
				MutableInt value = cityYearCounts.get(mapKey);
				value.increment();
			}
			count ++;
		}
		try {
			context.write(new Text("Year Count"), new IntWritable(count));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void cleanup(Context context) {
		try {
			TreeSet<CityYearFlights> sortedSet = new TreeSet<CityYearFlights>();
			Set<CityYearCombo> keyset = cityYearCounts.keySet();
			context.write(new Text("Keyset Size"), new IntWritable(keyset.size()));
			for (int currentYear = 1987; currentYear <= 2008; currentYear++) {
				for (CityYearCombo key : keyset) {
					if (key.year == currentYear) {
						context.write(new Text("Key Match"), new IntWritable(0));
						int fCount = cityYearCounts.get(key).get();
						CityYearFlights c = new CityYearFlights(fCount, key.city, key.year);
						sortedSet.add(c);
						if (sortedSet.size() > 10) {
							sortedSet.remove(sortedSet.first());
						}
					}
				}
				context.write(new Text("Sorted Set Size"), new IntWritable(sortedSet.size()));
				for (CityYearFlights c : sortedSet) {
					String keyOut = c.year + "-" + c.city;
					context.write(new Text(keyOut), new IntWritable(c.flights));
				}
				sortedSet.clear();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
