package com.srotya.sidewinder.core.storage;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Lock;

import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.compression.FilteredValueException;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.ValueWriter;

public interface Field {

	public void addDataPoint(Measurement measurement, long value) throws IOException;

	public void loadBucketMap(List<BufferObject> bufferEntries) throws IOException;

	public FieldReaderIterator queryReader(Predicate predicate, Lock readLock) throws IOException;

	/**
	 * Get {@link Reader} with time and value filter predicates pushed-down to it.
	 * 
	 * @param valuePredicate
	 * @return point in time instance of reader
	 * @throws IOException
	 */
	public static Reader getReader(ValueWriter writer, Predicate valuePredicate) throws IOException {
		Reader reader = writer.getReader();
		reader.setPredicate(valuePredicate);
		return reader;
	}

	public static void readerToPoints(List<Long> points, Reader reader) throws IOException {
		while (true) {
			try {
				try {
					long point = reader.read();
					points.add(point);
				} catch (FilteredValueException e) {
				}
			} catch (IOException e) {
				if (e instanceof RejectException) {
				} else {
					// getLo.log(Level.SEVERE, "Non rejectexception while reading datapoints", e);
				}
				break;
			}
		}
		if (reader.getCounter() != reader.getCount() || points.size() < reader.getCounter()) {
			// logger.finest(() -> "SDP:" + points.size() + "/" + reader.getCounter() + "/"
			// + reader.getCount());
		}
	}

}
