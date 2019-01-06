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
package com.srotya.sidewinder.core.rpc;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.srotya.sidewinder.core.rpc.WriterServiceGrpc.WriterServiceImplBase;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;

import io.grpc.stub.StreamObserver;

/**
 * @author ambud
 */
public class WriterServiceImpl extends WriterServiceImplBase {

	private static final String GRPC_DISRUPTOR_ENABLED = "grpc.disruptor.enabled";
	private static final String GRPC_DISRUPTOR_BUFFER_SIZE = "grpc.disruptor.buffer.size";
	private static final String GRPC_DISRUPTOR_HANDLER_COUNT = "grpc.disruptor.handler.count";
	private RingBuffer<DPWrapper> buffer;
	private Disruptor<DPWrapper> disruptor;
	private StorageEngine engine;
	private int handlerCount;
	private ExecutorService es;
	private boolean disruptorEnable;
	private DataPointTranslator translator;

	@SuppressWarnings("unchecked")
	public WriterServiceImpl(StorageEngine engine, Map<String, String> conf) {
		this.engine = engine;
		disruptorEnable = Boolean.parseBoolean(conf.getOrDefault(GRPC_DISRUPTOR_ENABLED, "false"));
		if (disruptorEnable) {
			int bufferSize = Integer.parseInt(conf.getOrDefault(GRPC_DISRUPTOR_BUFFER_SIZE, "65536"));
			if (bufferSize % 2 != 0) {
				throw new IllegalArgumentException("Disruptor buffers must always be power of 2");
			}
			translator = new DataPointTranslator();
			handlerCount = Integer.parseInt(conf.getOrDefault(GRPC_DISRUPTOR_HANDLER_COUNT, "2"));
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
	}

	@Override
	public void writeSingleDataPoint(SingleData request, StreamObserver<Ack> responseObserver) {
		Point point = request.getPoint();
		Ack ack = null;
		try {
			if (disruptorEnable) {
				buffer.publishEvent(translator, point, request.getMessageId(), null);
			} else {
				engine.writeDataPointWithLock(point, true);
			}
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
			List<Point> pointsList = request.getPointsList();
			for (int i = 0; i < pointsList.size(); i++) {
				Point point = pointsList.get(i);
				if (disruptorEnable) {
					buffer.publishEvent(translator, point, request.getMessageId(), null);
				} else {
					engine.writeDataPointWithLock(point, true);
				}
			}
			ack = Ack.newBuilder().setMessageId(request.getMessageId()).setResponseCode(200).build();
		} catch (Exception e) {
			e.printStackTrace();
			ack = Ack.newBuilder().setMessageId(request.getMessageId()).setResponseCode(500).build();
		}
		responseObserver.onNext(ack);
		responseObserver.onCompleted();
	}

	// @Override
	// public void writeSeriesPoint(RawTimeSeriesBucket request, StreamObserver<Ack>
	// responseObserver) {
	// Ack ack;
	// try {
	// TimeSeries series = engine.getOrCreateTimeSeries(request.getDbName(),
	// request.getMeasurementName(),
	// request.getValueFieldName(), new ArrayList<>(request.getTagsList()),
	// request.getBucketSize(),
	// request.getFp());
	// for (Bucket bucket : request.getBucketsList()) {
	// Writer writer = series.getOrCreateSeriesBucket(TimeUnit.MILLISECONDS,
	// bucket.getHeaderTimestamp());
	// writer.configure(null, false, 1, true);
	// writer.setCounter(bucket.getCount());
	// writer.bootstrap(bucket.getData().asReadOnlyByteBuffer());
	// }
	// ack =
	// Ack.newBuilder().setMessageId(request.getMessageId()).setResponseCode(200).build();
	// } catch (Exception e) {
	// ack =
	// Ack.newBuilder().setMessageId(request.getMessageId()).setResponseCode(500).build();
	// }
	// responseObserver.onNext(ack);
	// responseObserver.onCompleted();
	// }

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
		private Point dp;
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
		public Point getDp() {
			return dp;
		}

		/**
		 * @param dp
		 *            the dp to set
		 */
		public void setDp(Point dp) {
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
			Point dp = event.getDp();
			int hashCode = 0;
			for (Tag tag : dp.getTagsList()) {
				hashCode = hashCode * 31 + tag.hashCode();
			}
			event.setHashValue(hashCode);
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
					engine.writeDataPointWithoutLock(event.getDp(), true);
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
			dp.setDp(point);
			dp.setMessageId(messageId);
			dp.setResponseObserver(observer);
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