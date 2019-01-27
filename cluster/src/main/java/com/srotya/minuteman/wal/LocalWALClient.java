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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author ambud
 */
public class LocalWALClient extends WALClient {

	public static final String UNCOMMITTED = "uncommitted";
	public static final String COMMITTED = "committed";
	public static final String WAL_LOCAL_READ_MODE = "wal.local.read.mode";
	private static final Logger logger = Logger.getLogger(LocalWALClient.class.getName());
	private AtomicInteger counter;
	private boolean readCommitted;

	public LocalWALClient() {
	}

	public LocalWALClient configure(Map<String, String> conf, Integer nodeId, WAL localWAL, Object storageObject)
			throws IOException {
		super.configure(conf, nodeId, localWAL);
		this.counter = new AtomicInteger(0);
		String readMode = conf.getOrDefault(WAL_LOCAL_READ_MODE, UNCOMMITTED).toLowerCase();
		switch (readMode) {
		case COMMITTED:
			readCommitted = true;
			break;
		case UNCOMMITTED:
			readCommitted = false;
			break;
		}

		return this;
	}

	@Override
	public void iterate() {
		try {
			WALRead read = wal.read(nodeId, offset, maxFetchBytes, readCommitted);
			if (read.getData() == null || read.getData().isEmpty()) {
				Thread.sleep(retryWait);
			} else {
				logger.fine("Received read data:" + read.getData().size() + "\t" + offset);
				processData(read.getData());
				counter.incrementAndGet();
				logger.fine("Read:" + read.getData().size() + "\t\t\t" + offset + "\t\t\t" + wal.getCommitOffset());
			}
			offset = read.getNextOffset();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failure to replay WAL locally", e);
			try {
				Thread.sleep(errorRetryWait);
			} catch (InterruptedException e1) {
				stop();
			}
		}
	}

	public long getPos() {
		return wal.getCurrentOffset();
	}

	public WAL getWal() {
		return wal;
	}

	public void processData(List<byte[]> list) {
		// do nothing here
	}

}