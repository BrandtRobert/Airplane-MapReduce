package cs455.airlines;

public class MutableInt implements Comparable<MutableInt> {
	private static int idCount = 0;
	private int value;
	private final int uuid;
	
	public MutableInt(int value) {
		this.value = value;
		uuid = idCount;
		idCount++;
	}
	
	public void set(int value) {
		this.value = value;
	}
	
	public int get() {
		return value;
	}
	
	public void increment() {
		value++;
	}
	
	public void incrementBy(int value) {
		this.value += value;
	}
	
	public void decrement() {
		value++;
	}
	
	public String toString() {
		return value + "";
	}

	@Override
	/**
	 * Compared in descending order
	 */
	public int compareTo(MutableInt o) {
		if (this.equals(o)) {
			return 0;
		}
		if (this.get() > o.get()) {
			return 1;
		} else {
			return -1;
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + uuid;
		result = prime * result + value;
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
		MutableInt other = (MutableInt) obj;
		if (uuid != other.uuid) {
			return false;
		}
		if (value != other.value) {
			return false;
		}
		return true;
	}

}
