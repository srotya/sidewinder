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
package com.srotya.sidewinder.ingesters.graphite;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.MiscUtils;

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
		EmbeddedChannel ch = new EmbeddedChannel(new StringDecoder(), new GraphiteDecoder("test", engine, null));
		ch.writeInbound(
				Unpooled.copiedBuffer("app=1.server=1.s=jvm.heap.max 233123 1497720452", Charset.defaultCharset()));
		ch.readInbound();
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("app").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("server").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("s").setTagValue("jvm").build());
		verify(engine, times(1)).writeDataPointWithLock(
				MiscUtils.buildDataPoint("test", "heap", "max", tags, ((long) 1497720452) * 1000, 233123), false);
		ch.close();
	}

	@Test
	public void testParseAndInsert() throws IOException {
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("app").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("server").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("s").setTagValue("jvm").build());
		GraphiteDecoder.parseAndInsertDataPoints("test", "app=1.server=1.s=jvm.heap.max 233123 1497720452", engine);
		verify(engine, times(1)).writeDataPointWithLock(
				MiscUtils.buildDataPoint("test", "heap", "max", tags, ((long) 1497720452) * 1000, 233123), false);
	}

	@Test
	public void testIncorrectParseSkip() throws IOException {
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("app").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("server").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("s").setTagValue("jvm").build());
		GraphiteDecoder.parseAndInsertDataPoints("test", "app=1 1497720452", engine);
		GraphiteDecoder.parseAndInsertDataPoints("test", "app=1.app=2 1497720452", engine);

		GraphiteDecoder.parseAndInsertDataPoints("test",
				"app1.server1.jvm.heap.max233123 1497720452\n" + "app=1.server=2.s=jvm.heap.max 2331231497720452",
				engine);
		verify(engine, times(0)).writeDataPointWithLock(
				MiscUtils.buildDataPoint("test", "heap", "max", tags, ((long) 1497720452) * 1000, 233123), false);

		tags = Arrays.asList(Tag.newBuilder().setTagKey("server").setTagValue("2").build());
		GraphiteDecoder.parseAndInsertDataPoints("test", "app=1.server=1.heap 233123 1497720452", engine);
		verify(engine, times(0)).writeDataPointWithLock(
				MiscUtils.buildDataPoint("test", "heap", "max", tags, ((long) 1497720452) * 1000, 233123), false);
	}
}
