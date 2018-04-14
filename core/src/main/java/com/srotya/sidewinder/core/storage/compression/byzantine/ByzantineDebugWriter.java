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
package com.srotya.sidewinder.core.storage.compression.byzantine;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.compression.Codec;

/**
 * A simple delta-of-delta timeseries compression with XOR value compression
 * 
 * @author ambud
 */
@Codec(id = 10, name = "byzantine-debug")
public class ByzantineDebugWriter extends ByzantineWriter {

	private Timer timerWriteByzantine;

	@Override
	public void configure(ByteBuffer buf, boolean isNew, int startOffset, boolean isLocking) throws IOException {
		super.configure(buf, isNew, startOffset, isLocking);
		if (StorageEngine.ENABLE_METHOD_METRICS && MetricsRegistryService.getInstance() != null) {
			initMethodMetrics();
		}
	}

	private MetricRegistry initMethodMetrics() {
		MetricRegistry methodMetrics = MetricsRegistryService.getInstance().getInstance("method-metrics");
		timerWriteByzantine = methodMetrics.timer("writeByzantine");
		return methodMetrics;
	}

	@Override
	protected void writeDataPoint(long timestamp, long value) throws IOException {
		Context ctx = null;
		if (StorageEngine.ENABLE_METHOD_METRICS) {
			ctx = timerWriteByzantine.time();
		}
		try {
			super.writeDataPoint(timestamp, value);
		} finally {
			if (StorageEngine.ENABLE_METHOD_METRICS) {
				ctx.stop();
			}
		}
	}

}
