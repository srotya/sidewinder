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
package com.srotya.sidewinder.core.graphite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.codahale.metrics.Counter;
import com.srotya.sidewinder.core.storage.StorageEngine;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * http://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-plaintext-protocol
 * metricpath metricvalue metrictimestamp
 * 
 * @author ambud
 */
public class GraphiteDecoder extends SimpleChannelInboundHandler<String> {

	private static final Logger logger = Logger.getLogger(GraphiteDecoder.class.getName());
	private StorageEngine storageEngine;
	private String dbName;
	private Counter writeCounter;

	public GraphiteDecoder(String dbName, StorageEngine storageEngine, Counter writeCounter) {
		this.dbName = dbName;
		this.storageEngine = storageEngine;
		this.writeCounter = writeCounter;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
		logger.fine("Graphite input:" + msg);
		if (writeCounter != null) {
			writeCounter.inc();
		}
		parseAndInsertDataPoints(dbName, msg, storageEngine);
	}

	public static void parseAndInsertDataPoints(String dbName, String line, StorageEngine storageEngine)
			throws IOException {
		String[] parts = line.split("\\s+");
		if (parts.length != 3) {
			// invalid data point
			logger.fine("Ignoring bad metric:" + line);
			return;
		}
		String[] key = parts[0].split("\\.");
		String measurementName, valueFieldName;
		List<String> tags = new ArrayList<>();
		switch (key.length) {
		case 0:// invalid metric
		case 1:// invalid metric
		case 2:
			logger.fine("Ignoring bad metric:" + line);
			return;
		default:
			measurementName = key[key.length - 2];
			valueFieldName = key[key.length - 1];
			for (int i = 0; i < key.length - 2; i++) {
				tags.add(key[i]);
			}
			break;
		}
		long timestamp = Long.parseLong(parts[2]) * 1000;
		if (parts[1].contains(".")) {
			double value = Double.parseDouble(parts[1]);
			logger.fine("Writing graphite metric (fp)" + dbName + "," + measurementName + "," + valueFieldName + ","
					+ tags + "," + timestamp + "," + value);
			storageEngine.writeDataPoint(dbName, measurementName, valueFieldName, tags, timestamp, value);
		} else {
			long value = Long.parseLong(parts[1]);
			logger.fine("Writing graphite metric (fp)" + dbName + "," + measurementName + "," + valueFieldName + ","
					+ tags + "," + timestamp + "," + value);
			storageEngine.writeDataPoint(dbName, measurementName, valueFieldName, tags, timestamp, value);
		}
	}

}
