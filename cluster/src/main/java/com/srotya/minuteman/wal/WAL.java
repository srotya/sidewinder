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
package com.srotya.minuteman.wal;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author ambud
 */
public interface WAL {

	public static final String DEFAULT_WAL_DIR = "target/wal";
	public static final int DEFAULT_WALL_SIZE = 200 * 1024 * 1024; // 200MB
	public static final String WAL_SEGMENT_FLUSH_COUNT = "wal.segment.flush.count";
	public static final String WAL_SEGMENT_SIZE = "wal.segment.size";
	public static final String WAL_DIR = "wal.dir";
	public static final String WAL_ISR_THRESHOLD = "wal.isr.threshold";
	public static final String WAL_DELETION_DISABLED = "wal.deletion.disabled";
	public static final String WAL_ISRCHECK_FREQUENCY = "wal.isrcheck.frequency";
	public static final String WAL_ISRCHECK_DELAY = "wal.isrcheck.delay";

	void configure(Map<String, String> conf, ScheduledExecutorService es) throws IOException;

	void flush() throws IOException;

	int getOffset() throws IOException;

	void write(byte[] data, boolean flush) throws IOException;

	WALRead read(Integer followerId, long offset, int maxBytes, boolean readCommitted) throws IOException;

	long getCurrentOffset();

	long getFollowerOffset(Integer followerId);

	Collection<Integer> getFollowers();

	void setCommitOffset(long commitOffset);

	long getCommitOffset();

	boolean isIsr(Integer followerId);

	void setWALDeletion(boolean status);

	void close() throws IOException;

	int getSegmentCounter();

}
