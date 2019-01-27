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
package com.srotya.minuteman.wal;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class WALClient implements Runnable{
	
	public static final int DEFAULT_MAX_FETCH_BYTES = 1024 * 1024;
	public static final String DEFAULT_WAL_CLIENT_EMPTY_WAIT = "2000";
	public static final String DEFAULT_WAL_CLIENT_ERROR_WAIT = "1";
	public static final String WAL_CLIENT_ERROR_WAIT = "wal.client.error.wait";
	public static final String WAL_CLIENT_EMPTY_WAIT = "wal.client.empty.wait";
	public static final String MAX_FETCH_BYTES = "max.fetch.bytes";
	private AtomicBoolean ctrl;
	protected int retryWait;
	protected int errorRetryWait;
	protected String nodeId;
	protected WAL wal;
	protected int maxFetchBytes;
	protected long offset;

	public abstract void iterate();

	@Override
	public void run() {
		while (ctrl.get()) {
			iterate();
		}
	}

	public WALClient configure(Map<String, String> conf, String nodeId, WAL localWAL) throws IOException {
		if (localWAL == null || nodeId == null || conf == null) {
			throw new IllegalArgumentException("Arguments can't be null:" + localWAL + "," + nodeId + "," + conf);
		}
		this.ctrl = new AtomicBoolean(true);
		retryWait = Integer
				.parseInt(conf.getOrDefault(WAL_CLIENT_EMPTY_WAIT, DEFAULT_WAL_CLIENT_EMPTY_WAIT));
		errorRetryWait = Integer
				.parseInt(conf.getOrDefault(WAL_CLIENT_ERROR_WAIT, DEFAULT_WAL_CLIENT_ERROR_WAIT));
		this.wal = localWAL;
		this.nodeId = nodeId;
		this.offset = wal.getCurrentOffset();
		this.maxFetchBytes = Integer.parseInt(conf.getOrDefault(MAX_FETCH_BYTES, String.valueOf(1024 * 1024)));
		return this;
	}
	
	public AtomicBoolean getCtrl() {
		return ctrl;
	}
	
	public void stop() {
		ctrl.set(false);
	}
}
