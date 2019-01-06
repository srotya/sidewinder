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
package com.srotya.sidewinder.ingesters.statsd;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.charset.Charset;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.storage.StorageEngine;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.string.StringDecoder;

@RunWith(MockitoJUnitRunner.class)
public class TestStatsdDecoder {

	@Mock
	private StorageEngine engine;

	@Test
	public void testStatsdParse() throws IOException {
		EmbeddedChannel ch = new EmbeddedChannel(new StringDecoder(), new StatdsDecoder("test", engine, null));
		ch.writeInbound(Unpooled.copiedBuffer("http.server_ngnix.latency:1121|ms", Charset.defaultCharset()));
		ch.readInbound();
		verify(engine, times(1)).writeDataPointWithLock(any(Point.class), anyBoolean());
		// MiscUtils.buildDataPoint("test", "http", "latency", tags,
		// System.currentTimeMillis(), 1121), false);
		ch.close();
	}

}
