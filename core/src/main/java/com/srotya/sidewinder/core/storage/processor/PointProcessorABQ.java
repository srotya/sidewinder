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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
@SuppressWarnings("unused")
public class PointProcessorABQ implements PointProcessor {

	private int handlerCount;
	private ExecutorService es;
	private Map<Integer, ArrayBlockingQueue<Point>> queue;
	private StorageEngine engine;

	public PointProcessorABQ(final StorageEngine engine, Map<String, String> conf) {
		this.engine = engine;
		queue = new HashMap<>();
		handlerCount = 4;
		// es = Executors.newFixedThreadPool(handlerCount + 1, new
		// BackgrounThreadFactory("writers"));

		// es.submit(() -> {
		// while (true) {
		// queue.entrySet().stream().forEach(v -> System.out.println(v.getKey() + " : "
		// + v.getValue().size()));
		// Thread.sleep(1000);
		// }
		// });

		// for (int i = 0; i < handlerCount; i++) {
		// System.out.println("Initializaing handler:" + i);
		// final ArrayBlockingQueue<Point> value = new ArrayBlockingQueue<>(1024 * 128);
		// queue.put(i, value);
		// es.submit(() -> {
		// while (true) {
		// Point take = value.take();
		// engine.writeDataPointUnlocked(take, true);
		// }
		// });
		// }
	}

	public void writeDataPoint(Point point) throws InterruptedException, IOException {
		engine.writeDataPointWithLock(point, true);
		// extracted(point);
	}

	private void extracted(Point point) throws InterruptedException {
		int hashCode = MiscUtils.tagHashCode(point.getTagsList());
		queue.get(Math.abs(hashCode % handlerCount)).put(point);
	}

	/**
	 * @return the es
	 */
	public ExecutorService getEs() {
		return es;
	}

}