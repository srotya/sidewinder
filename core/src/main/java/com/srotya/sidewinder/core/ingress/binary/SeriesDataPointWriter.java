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
package com.srotya.sidewinder.core.ingress.binary;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.StorageEngine;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

/**
 * Netty Channel writer for Sidewinder binary protocol
 * 
 * @author ambud
 */
public class SeriesDataPointWriter extends ChannelInboundHandlerAdapter {

	private static final Logger logger = Logger.getLogger(SeriesDataPointWriter.class.getName());
	private static AtomicLong counter = new AtomicLong();
	private StorageEngine engine;

	public SeriesDataPointWriter(StorageEngine engine) {
		this.engine = engine;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		if (msg == null) {
			return;
		}
		DataPoint dp = (DataPoint) msg;
		try {
			engine.writeDataPoint(dp);
			if (counter.incrementAndGet() % 1000000 == 0) {
				System.out.println(counter.get());
			}
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Error writing data point", e);
		}
		ReferenceCountUtil.release(msg);
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
	}

}
