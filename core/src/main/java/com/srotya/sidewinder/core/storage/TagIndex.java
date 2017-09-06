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
import java.util.Set;

/**
 * @author ambud
 */
public interface TagIndex {
	
	/**
	 * @param tag
	 * @return uid
	 * @throws IOException
	 */
	public String createEntry(String tag) throws IOException;
	
	public String getEntry(String hexString) throws IOException;
	
	public Set<String> getTags() throws IOException;
	
	/**
	 * Indexes tag in the row key, creating an adjacency list
	 * 
	 * @param tag
	 * @param rowKey
	 * @throws IOException
	 */
	public void index(String tag, String rowKey) throws IOException;

	/**
	 * @param tag
	 * @return
	 * @throws IOException 
	 */
	public Set<String> searchRowKeysForTag(String tag) throws IOException;

	public void close() throws IOException;

}
