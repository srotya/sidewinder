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
package com.srotya.sidewinder.core.storage.mem.archival;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.srotya.sidewinder.core.storage.mem.ArchiveException;
import com.srotya.sidewinder.core.storage.mem.Archiver;

/**
 * @author ambud
 */
public class NoneArchiver implements Archiver {

	@Override
	public void init(Map<String, String> conf) throws IOException {
	}

	@Override
	public void archive(TimeSeriesArchivalObject object) throws ArchiveException {
		// do nothing
	}

	@Override
	public List<TimeSeriesArchivalObject> unarchive() throws ArchiveException {
		// TODO Auto-generated method stub
		return null;
	}

}
