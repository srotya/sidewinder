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
package com.srotya.sidewinder.core.rpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.srotya.sidewinder.core.rpc.WriterServiceGrpc.WriterServiceImplBase;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TimeSeries;
import com.srotya.sidewinder.core.storage.compression.Writer;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;
import com.srotya.sidewinder.core.utils.MiscUtils;

import io.grpc.stub.StreamObserver;

/**
 * @author ambud
 */
public class WriterServiceImpl extends WriterServiceImplBase {

	private RingBuffer<DPWrapper> buffer;
	private Disruptor<DPWrapper> disruptor;
	private StorageEngine engine;
	private Map<String, String> conf;
	private int handlerCount;
	private ExecutorService es;

	@SuppressWarnings("unchecked")
	public WriterServiceImpl(StorageEngine engine, Map<String, String> conf) {
		this.engine = engine;
		this.conf = conf;
		int bufferSize = Integer.parseInt(conf.getOrDefault("grpc.disruptor.buffer.size", "16384"));
		if (bufferSize % 2 != 0) {
			throw new IllegalArgumentException("Disruptor buffers must always be power of 2");
		}
		handlerCount = Integer.parseInt(conf.getOrDefault("grpc.disruptor.handler.count", "2"));
		es = Executors.newFixedThreadPool(handlerCount + 2, new BackgrounThreadFactory("grpc-writers"));
		disruptor = new Disruptor<>(new DPWrapperFactory(), bufferSize, es);
		@SuppressWarnings("rawtypes")
		EventHandler[] handlers = new EventHandler[handlerCount];
		for (int i = 0; i < handlerCount; i++) {
			handlers[i] = new WriteHandler(engine, handlerCount, i);
		}
		disruptor.handleEventsWith(new HashHandler()).then(handlers);
		buffer = disruptor.start();
	}

	@Override
	public void writeSingleDataPoint(SingleData request, StreamObserver<Ack> responseObserver) {
		Point point = request.getPoint();
		Ack ack = null;
		try {
			// buffer.publishEvent(translator, point, point.getTimestamp(), null);
			engine.writeDataPoint(MiscUtils.pointToDataPoint(point));
			ack = Ack.newBuilder().setMessageId(request.getMessageId()).setResponseCode(200).build();
		} catch (Exception e) {
			ack = Ack.newBuilder().setMessageId(request.getMessageId()).setResponseCode(500).build();
		}
		responseObserver.onNext(ack);
		responseObserver.onCompleted();
	}

	@Override
	public void writeBatchDataPoint(BatchData request, StreamObserver<Ack> responseObserver) {
		Ack ack = null;
		try {
			for (Point point : request.getPointsList()) {
				// buffer.publishEvent(translator, point, point.getTimestamp(), null);
				engine.writeDataPoint(MiscUtils.pointToDataPoint(point));
			}
			ack = Ack.newBuilder().setMessageId(request.getMessageId()).setResponseCode(200).build();
		} catch (Exception e) {
			ack = Ack.newBuilder().setMessageId(request.getMessageId()).setResponseCode(500).build();
		}
		responseObserver.onNext(ack);
		responseObserver.onCompleted();
	}

	@Override
	public void writeSeriesPoint(RawTimeSeriesBucket request, StreamObserver<Ack> responseObserver) {
		Ack ack;
		try {
			TimeSeries series = engine.getOrCreateTimeSeries(request.getDbName(), request.getMeasurementName(),
					request.getValueFieldName(), new ArrayList<>(request.getTagsList()), request.getBucketSize(),
					request.getFp());
			for (Bucket bucket : request.getBucketsList()) {
				Writer writer = series.getOrCreateSeriesBucket(TimeUnit.MILLISECONDS, bucket.getHeaderTimestamp());
				writer.configure(conf, null, false, 1, true);
				writer.setCounter(bucket.getCount());
				writer.bootstrap(bucket.getData().asReadOnlyByteBuffer());
			}
			ack = Ack.newBuilder().setMessageId(request.getMessageId()).setResponseCode(200).build();
		} catch (Exception e) {
			ack = Ack.newBuilder().setMessageId(request.getMessageId()).setResponseCode(500).build();
		}
		responseObserver.onNext(ack);
		responseObserver.onCompleted();
	}

	@Override
	public StreamObserver<SingleData> writeDataPointStream(final StreamObserver<Ack> responseObserver) {
		return new WriteStreamObserver(buffer, responseObserver);
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

		private long messageId;
		private DataPoint dp;
		private StreamObserver<Ack> responseObserver;
		private int hashValue;

		/**
		 * @return the messageId
		 */
		public long getMessageId() {
			return messageId;
		}

		/**
		 * @param messageId
		 *            the messageId to set
		 */
		public void setMessageId(long messageId) {
			this.messageId = messageId;
		}

		/**
		 * @return the dp
		 */
		public DataPoint getDp() {
			return dp;
		}

		/**
		 * @param dp
		 *            the dp to set
		 */
		public void setDp(DataPoint dp) {
			this.dp = dp;
		}

		/**
		 * @return the responseObserver
		 */
		public StreamObserver<Ack> getResponseObserver() {
			return responseObserver;
		}

		/**
		 * @param responseObserver
		 *            the responseObserver to set
		 */
		public void setResponseObserver(StreamObserver<Ack> responseObserver) {
			this.responseObserver = responseObserver;
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

	public static class HashHandler implements EventHandler<DPWrapper> {

		@Override
		public void onEvent(DPWrapper event, long sequence, boolean endOfBatch) throws Exception {
			StringBuilder bufString = new StringBuilder();
			DataPoint dp = event.getDp();
			bufString.append(dp.getDbName() + "\n");
			bufString.append(dp.getMeasurementName() + "\n");
			bufString.append(dp.getValueFieldName() + "\n");
			bufString.append(dp.getTags().toString() + "\n");
			event.setHashValue(bufString.toString().hashCode());
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
				Ack ack = null;
				try {
					engine.writeDataPoint(event.getDp());
					ack = Ack.newBuilder().setMessageId(event.getMessageId()).setResponseCode(200).build();
				} catch (IOException e) {
					ack = Ack.newBuilder().setMessageId(event.getMessageId()).setResponseCode(400).build();
				} catch (Exception e) {
					ack = Ack.newBuilder().setMessageId(event.getMessageId()).setResponseCode(500).build();
				}
				if (event.getResponseObserver() != null) {
					event.getResponseObserver().onNext(ack);
					event.getResponseObserver().onCompleted();
				}
			}
		}

	}

	public static class WriteStreamObserver implements StreamObserver<SingleData> {

		private RingBuffer<DPWrapper> ring;
		private DataPointTranslator translator;
		private StreamObserver<Ack> responseObserver;

		public WriteStreamObserver(RingBuffer<DPWrapper> wrapper, StreamObserver<Ack> responseObserver) {
			this.ring = wrapper;
			this.responseObserver = responseObserver;
			this.translator = new DataPointTranslator();
		}

		@Override
		public void onNext(SingleData value) {
			ring.publishEvent(translator, value.getPoint(), value.getMessageId(), responseObserver);
		}

		@Override
		public void onError(Throwable t) {
		}

		@Override
		public void onCompleted() {
		}

	}

	public static class DataPointTranslator
			implements EventTranslatorThreeArg<DPWrapper, Point, Long, StreamObserver<Ack>> {

		@Override
		public void translateTo(DPWrapper dp, long sequence, Point point, Long messageId,
				StreamObserver<Ack> observer) {
			MiscUtils.pointToDataPoint(dp.getDp(), point);
			dp.setMessageId(messageId);
			dp.setResponseObserver(observer);
		}

	}

	public static class DPWrapperFactory implements EventFactory<DPWrapper> {

		@Override
		public DPWrapper newInstance() {
			DPWrapper wrapper = new DPWrapper();
			wrapper.setDp(new DataPoint());
			return wrapper;
		}

	}

}