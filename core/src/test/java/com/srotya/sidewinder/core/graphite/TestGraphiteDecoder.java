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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.srotya.sidewinder.core.storage.StorageEngine;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.string.StringDecoder;

/**
 * @author ambud
 */
@RunWith(MockitoJUnitRunner.class)
public class TestGraphiteDecoder {

	@Mock
	private StorageEngine engine;

	@Test
	public void testHandler() throws IOException {
		EmbeddedChannel ch = new EmbeddedChannel(new StringDecoder(), new GraphiteDecoder("test", engine));
		ch.writeInbound(Unpooled.copiedBuffer("app1.server1.jvm.heap.max 233123 1497720452", Charset.defaultCharset()));
		ch.readInbound();
		verify(engine, times(1)).writeDataPoint("test", "heap", "max", Arrays.asList("app1", "server1", "jvm"),
				((long) 1497720452) * 1000, 233123);
		ch.close();
	}

	@Test
	public void testParseAndInsert() throws IOException {
		GraphiteDecoder.parseAndInsertDataPoints("test", "app1.server1.jvm.heap.max 233123 1497720452", engine);
		verify(engine, times(1)).writeDataPoint("test", "heap", "max", Arrays.asList("app1", "server1", "jvm"),
				((long) 1497720452) * 1000, 233123);
	}

	@Test
	public void testIncorrectParseSkip() throws IOException {
		GraphiteDecoder.parseAndInsertDataPoints("test", "app1 1497720452", engine);
		GraphiteDecoder.parseAndInsertDataPoints("test", "app1.app2 1497720452", engine);

		GraphiteDecoder.parseAndInsertDataPoints("test",
				"app1.server1.jvm.heap.max233123 1497720452\n" + "app1.server2.jvm.heap.max 2331231497720452", engine);
		verify(engine, times(0)).writeDataPoint("test", "heap", "max", Arrays.asList("app1", "server2", "jvm"),
				((long) 1497720452) * 1000, 233123);

		GraphiteDecoder.parseAndInsertDataPoints("test", "app1.server1.heap 233123 1497720452", engine);
		verify(engine, times(0)).writeDataPoint("test", "heap", "max", Arrays.asList("server2"),
				((long) 1497720452) * 1000, 233123);
	}
}
