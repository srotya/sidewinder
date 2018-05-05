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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LinkedByteString {

	private List<ByteString> stringList;

	public LinkedByteString() {
		stringList = new ArrayList<>();
	}

	public LinkedByteString(int size) {
		stringList = new ArrayList<>(size);
	}

	public LinkedByteString(ByteString... strings) {
		this();
		for (ByteString str : strings) {
			stringList.add(str);
		}
	}

	public LinkedByteString(String... strings) {
		this();
		for (String str : strings) {
			stringList.add(new ByteString(str));
		}
	}
	
	public ByteString get(int index) {
		return stringList.get(index);
	}
	
	public List<ByteString> getStringList() {
		return stringList;
	}

	public LinkedByteString concat(ByteString str) {
		stringList.add(str);
		return this;
	}

	public LinkedByteString concat(String str) {
		stringList.add(new ByteString(str));
		return this;
	}

	public LinkedByteString concat(LinkedByteString str) {
		for (ByteString s : str.stringList) {
			stringList.add(s);
		}
		return this;
	}

	@Override
	public int hashCode() {
		int h = 0;
		for (int i = 0; i < stringList.size(); i++) {
			h = h * 31 + stringList.get(i).hashCode();
		}
		return h;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (obj instanceof LinkedByteString) {
			LinkedByteString other = ((LinkedByteString) obj);
			if (other.stringList.size() != this.stringList.size()) {
				return false;
			} else {
				for (int i = 0; i < this.stringList.size(); i++) {
					if (!other.stringList.get(i).equals(this.stringList.get(i))) {
						return false;
					}
				}
				return true;
			}
		}
		return false;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (ByteString entry : stringList) {
			builder.append(entry.toString());
		}
		return builder.toString();
	}

	public byte[] getBytes() {
		int capacity = 0;
		for (ByteString entry : stringList) {
			capacity += entry.length();
		}
		ByteBuffer buf = ByteBuffer.allocate(capacity);
		for (ByteString entry : stringList) {
			buf.put(entry.getBytes());
		}
		return buf.array();
	}

}
