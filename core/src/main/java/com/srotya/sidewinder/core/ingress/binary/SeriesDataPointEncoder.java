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

import java.util.List;

import com.srotya.sidewinder.core.storage.DataPoint;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @author ambud
 */
public class SeriesDataPointEncoder extends MessageToByteEncoder<List<DataPoint>> {

	@Override
	protected void encode(ChannelHandlerContext arg0, List<DataPoint> dataPoints, ByteBuf buf) throws Exception {
		int size = dataPoints.size();
		buf.writeInt(size);
		for (DataPoint dataPoint : dataPoints) {
			encodeDPointToBuf(buf, dataPoint);
			arg0.flush();
		}
	}

	public static void encodeDPointToBuf(ByteBuf buf, DataPoint dataPoint) {
		byte[] dbNameBytes = dataPoint.getDbName().getBytes();
		buf.writeInt(dbNameBytes.length);
		buf.writeBytes(dbNameBytes);
		byte[] measurementNameBytes = dataPoint.getMeasurementName().getBytes();
		buf.writeInt(measurementNameBytes.length);
		buf.writeBytes(measurementNameBytes);
		byte[] valueNameBytes = dataPoint.getValueFieldName().getBytes();
		buf.writeInt(valueNameBytes.length);
		buf.writeBytes(valueNameBytes);

		List<String> tags = dataPoint.getTags();
		buf.writeInt(tags.size());
		for (String tag : tags) {
			byte[] value = tag.getBytes();
			buf.writeInt(value.length);
			buf.writeBytes(value);
		}

		buf.writeLong(dataPoint.getTimestamp());
		if (dataPoint.isFp()) {
			buf.writeByte('0');
			buf.writeDouble(dataPoint.getValue());
		} else {
			buf.writeByte('1');
			buf.writeLong(dataPoint.getLongValue());
		}
	}

}
