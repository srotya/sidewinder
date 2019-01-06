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
import java.util.Iterator;

import com.srotya.sidewinder.core.storage.compression.FilteredValueException;

/**
 * 
 * 
 * @author ambud
 */
public class DataPointIterator implements Iterator<DataPoint> {

	private FieldReaderIterator timeIterator;
	private FieldReaderIterator valueIterator;
	private DataPoint dp = new DataPoint();
	private boolean read = true;
	Double test;

	protected DataPointIterator() {
	}

	public DataPointIterator(FieldReaderIterator timeIterator, FieldReaderIterator valueIterator) {
		this.timeIterator = timeIterator;
		this.valueIterator = valueIterator;
	}

	@Override
	public boolean hasNext() {
		if (!read) {
			return true;
		}
		while (true) {
			try {
				long time = timeIterator.next();
				long value = valueIterator.next();
				dp.setTimestamp(time);
				dp.setLongValue(value);
				read = false;
				return true;
			} catch (FilteredValueException e) {
				continue;
			} catch (IOException e) {
				this.read = true;
				return false;
			}
		}
	}

	@Override
	public DataPoint next() {
		read = true;
		return dp;
	}

	public void prev() {
		read = false;
	}

}