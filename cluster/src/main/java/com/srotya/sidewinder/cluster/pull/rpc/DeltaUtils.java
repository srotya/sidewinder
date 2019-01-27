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
package com.srotya.sidewinder.cluster.pull.rpc;

import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.srotya.sidewinder.cluster.rpc.DeltaObject;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.Series;
import com.srotya.sidewinder.core.storage.compression.Writer;

public class DeltaUtils {

	public static final Logger logger = Logger.getLogger(DeltaUtils.class.getName());

	public static DeltaObject buildAndAddDeltaObject(List<Tag> tags, String field,
			Entry<Integer, List<Writer>> entry, Series timeSeries) {
		Integer bucket = entry.getKey();
		List<Writer> value = entry.getValue();
		DeltaObject.Builder delta = DeltaObject.newBuilder();
		delta.addAllTags(tags);
		delta.setValueFieldName(field);
		delta.setBucket(bucket);
//		delta.setFp(timeSeries.isFp());
//		delta.setBucketSize(timeSeries.getTimeBucketSize());
		// skip sending delta info for writers that are readonly or full since open
		// writers can't be repaired
		checkAndPrintDebugInfo(field, bucket, value);
		delta.addAllWriterDataPointCount(value.stream().filter(v -> v.isFull() | v.isReadOnly())
				.mapToInt(w -> w.getCount()).boxed().collect(Collectors.toList()));
		delta.addAllWriterBufferSize(value.stream().filter(v -> v.isFull() | v.isReadOnly())
				.mapToInt(w -> w.getPosition()).boxed().collect(Collectors.toList()));
		delta.setWriterCount(delta.getWriterBufferSizeList().size());
		if (delta.getWriterCount() > 0) {
			return delta.build();
		} else {
			return null;
		}
	}

	public static void checkAndPrintDebugInfo(String field, Integer bucket, List<Writer> value) {
		if (logger.isLoggable(Level.FINE)) {
			List<String> collect = value.stream()
					.map(v -> "field: " + field + " " + bucket + " " + v.isFull() + " " + v.isReadOnly())
					.collect(Collectors.toList());
			logger.finest("delta tsbuckets:" + collect);
		}
	}

}
