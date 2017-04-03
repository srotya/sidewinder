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

import java.util.ArrayList;
import java.util.List;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.utils.MiscUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * Netty decoder for Sidewinder binary protocol
 * 
 * @author ambud
 */
public class SeriesDataPointDecoder extends ByteToMessageDecoder {

	@Override
	protected void decode(ChannelHandlerContext arg0, ByteBuf buf, List<Object> output) throws Exception {
		buf.retain();
		int dpCount = buf.readInt();
		for (int i = 0; i < dpCount; i++) {
			DataPoint d = decodeBufToDPoint(buf);
			if (d == null) {
				System.out.println("Bad data point");
				return;
			} else {
				output.add(d);
			}
		}
		buf.release();
	}

	public static DataPoint decodeBufToDPoint(ByteBuf buf) {
		int dbNameLength = buf.readInt();
		if (dbNameLength < 0) {
			return null;
		}
		byte[] dbBytes = new byte[dbNameLength];
		buf.readBytes(dbBytes);
		String dbName = new String(dbBytes);
		int measurementNameLength = buf.readInt();
		if (measurementNameLength < 0) {
			return null;
		}
		byte[] measurementNameBytes = new byte[measurementNameLength];
		buf.readBytes(measurementNameBytes);
		String measurementName = new String(measurementNameBytes);

		int valueNameLength = buf.readInt();
		if (valueNameLength < 0) {
			return null;
		}
		byte[] valueNameBytes = new byte[valueNameLength];
		buf.readBytes(valueNameBytes);
		String valueFieldName = new String(valueNameBytes);

		List<String> tags = new ArrayList<>();

		int tagCount = buf.readInt();
		for (int i = 0; i < tagCount; i++) {
			byte[] value = new byte[buf.readInt()];
			buf.readBytes(value);
			tags.add(new String(value));
		}

		long timestamp = buf.readLong();
		byte flag = buf.readByte();
		DataPoint dp;
		if (flag == '0') {
			double value = buf.readDouble();
			dp = MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName, tags, timestamp, value);
			dp.setFp(true);
		} else {
			long value = buf.readLong();
			dp = MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName, tags, timestamp, value);
		}
		return dp;
	}

}
