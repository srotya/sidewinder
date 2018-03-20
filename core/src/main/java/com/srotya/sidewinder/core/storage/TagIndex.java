/**
 * Copyright 2017 Ambud Sharma
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
import java.util.Collection;
import java.util.Set;

import com.srotya.sidewinder.core.filters.TagFilter;

/**
 * @author ambud
 */
public interface TagIndex {

	public Collection<String> getTagKeys() throws IOException;

	@Deprecated
	public void index(String tag, String value, String rowKey) throws IOException;

	/**
	 * Indexes tag in the row key
	 * 
	 * @param tag
	 * @param value
	 * @param rowIndex
	 * @throws IOException
	 */
	public void index(String tag, String value, int rowIndex) throws IOException;

	public void close() throws IOException;

	public int getSize();

	public Set<ByteString> searchRowKeysForTagFilter(TagFilter tagFilterTree);

	public Collection<String> getTagValues(String tagKey);

	public static Set<ByteString> stringSetToByteSet(Set<String> input, Set<ByteString> output) {
		if (input != null) {
			for (String s : input) {
				output.add(new ByteString(s));
			}
		}
		return output;
	}

	public static Set<String> byteToStringSet(Set<ByteString> input, Set<String> output) {
		if (input != null) {
			for (ByteString s : input) {
				output.add(s.toString());
			}
		}
		return output;
	}

}
