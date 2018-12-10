/**
 * Copyright Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.sidewinder.core.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.srotya.sidewinder.core.storage.compression.Reader;

public class FieldReaderIterator {

	private int idx;
	private List<Reader> readers;
	private String fieldName;
	
	public FieldReaderIterator() {
		readers = new ArrayList<>();
	}

	public FieldReaderIterator(String fieldName) {
		this.fieldName = fieldName;
		readers = new ArrayList<>();
	}

	public FieldReaderIterator addReader(List<Reader> readers) {
		this.readers.addAll(readers);
		return this;
	}

	public long next() throws IOException {
		try {
			return readers.get(idx).read();
		} catch (RejectException e) {
			if (idx < readers.size() - 1) {
				idx++;
				return next();
			} else {
				throw e;
			}
		}
	}

	public List<Reader> getReaders() {
		return readers;
	}

	public static long[] extracted(FieldReaderIterator[] iterators) throws IOException {
		IOException exception = null;
		long[] tuple = new long[iterators.length];
		for (int i = 0; i < iterators.length; i++) {
			if (iterators[i] != null && iterators[i].readers.size() != 0) {
				try {
					tuple[i] = iterators[i].next();
				} catch (IOException e) {
					exception = e;
				}
			}
		}
		if (exception != null) {
			throw exception;
		}
		return tuple;
	}

	public static Object[] extractedObject(FieldReaderIterator[] iterators, int rowCounter, List<Boolean> fTypes)
			throws IOException {
		Object[] tuple = new Object[iterators.length];
		IOException exception = null;
		for (int i = 0; i < iterators.length; i++) {
			if (iterators[i] != null && iterators[i].readers.size() != 0) {
				try {
					long next = iterators[i].next();
					if (!fTypes.get(i)) {
						tuple[i] = next;
					} else {
						tuple[i] = Double.longBitsToDouble(next);
					}
				} catch (IOException e) {
					exception = e;
				}
			}
		}
		if (exception != null) {
			throw exception;
		}
		return tuple;
	}

	public int count() {
		return readers.stream().mapToInt(r -> r.getCount()).sum();
	}

	public String getFieldName() {
		return fieldName;
	}
	
	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "FieldReaderIterator [idx=" + idx + ", readers=" + readers + ", fieldName=" + fieldName + "]";
	}
}
