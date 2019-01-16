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
package com.srotya.sidewinder.core.storage.processor;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;

/**
 * @author ambud
 */
public class PointProcessorDisruptor implements PointProcessor {

	private RingBuffer<DPWrapper> buffer;
	private Disruptor<DPWrapper> disruptor;
	private int handlerCount;
	private ExecutorService es;
	private DataPointTranslator translator;

	@SuppressWarnings("unchecked")
	public PointProcessorDisruptor(StorageEngine engine, Map<String, String> conf) {
		int bufferSize = 65536 * 4;
		if (bufferSize % 2 != 0) {
			throw new IllegalArgumentException("Disruptor buffers must always be power of 2");
		}
		translator = new DataPointTranslator();
		handlerCount = 2;
		es = Executors.newFixedThreadPool(handlerCount, new BackgrounThreadFactory("grpc-writers"));
		disruptor = new Disruptor<>(new DPWrapperFactory(), bufferSize, es);
		@SuppressWarnings("rawtypes")
		EventHandler[] handlers = new EventHandler[handlerCount];
		for (int i = 0; i < handlerCount; i++) {
			handlers[i] = new WriteHandler(engine, handlerCount, i);
		}
		disruptor.handleEventsWith(handlers);
		buffer = disruptor.start();
	}

	public void writeDataPoint(Point point) {
		buffer.publishEvent(translator, point.getDbName(), point);
	}

	/**
	 * @return the disruptor
	 */
	public Disruptor<DPWrapper> getDisruptor() {
		return disruptor;
	}

	/**
	 * @return the es
	 */
	public ExecutorService getEs() {
		return es;
	}

	public static class DPWrapper {

		private Point dp;
		private int hashValue;

		/**
		 * @return the dp
		 */
		public Point getDp() {
			return dp;
		}

		public void setDp(Point dp) {
			this.dp = dp;
		}
		
		/**
		 * @return the hashValue
		 */
		public int getHashValue() {
			return hashValue;
		}

		/**
		 * @param hashValue
		 *            the hashValue to set
		 */
		public void setHashValue(int hashValue) {
			this.hashValue = hashValue;
		}

	}

	public static class WriteHandler implements EventHandler<DPWrapper> {

		private StorageEngine engine;
		private int handlerCount;
		private int handlerIndex;

		public WriteHandler(StorageEngine engine, int handlerCount, int handlerIndex) {
			this.engine = engine;
			this.handlerCount = handlerCount;
			this.handlerIndex = handlerIndex;
		}

		@Override
		public void onEvent(DPWrapper event, long sequence, boolean endOfBatch) throws Exception {
			if (event.getHashValue() % handlerCount == handlerIndex) {
				try {
					engine.writeDataPointWithLock(event.getDp(), true);
				} catch (IOException e) {
				} catch (Exception e) {
				}
			}
		}

	}

	public static class DataPointTranslator implements EventTranslatorTwoArg<DPWrapper, String, Point> {

		@Override
		public void translateTo(DPWrapper dp, long arg1, String dbName, Point point) {
			int hashCode = 0;
			for (Tag tag : point.getTagsList()) {
				hashCode = hashCode * 31 + tag.hashCode();
			}
			dp.setHashValue(hashCode);
		}

	}

	public static class DPWrapperFactory implements EventFactory<DPWrapper> {

		@Override
		public DPWrapper newInstance() {
			DPWrapper wrapper = new DPWrapper();
			return wrapper;
		}

	}

}