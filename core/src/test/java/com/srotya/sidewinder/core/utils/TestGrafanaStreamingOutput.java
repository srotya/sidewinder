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
package com.srotya.sidewinder.core.utils;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import javax.ws.rs.WebApplicationException;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.srotya.sidewinder.core.api.grafana.GrafanaOutputv2;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.MockDataPointIterator;

public class TestGrafanaStreamingOutput {

	@Test
	public void testJsonValidity() throws WebApplicationException, IOException {
		GrafanaOutputv2 out1 = new GrafanaOutputv2("targetname1", false);
		out1.setPointsIterator(new MockDataPointIterator(Arrays.asList(new DataPoint(1L, 2L), new DataPoint(3L, 4L))));
		GrafanaStreamingOutput output = new GrafanaStreamingOutput(Arrays.asList(Arrays.asList(out1).iterator()));
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		output.write(os);
		String payload = new String(os.toByteArray());
		JsonArray ary = new Gson().fromJson(payload, JsonArray.class);
		assertEquals(1, ary.size());
		assertEquals(2, ary.get(0).getAsJsonObject().get("datapoints").getAsJsonArray().size());

		out1.setPointsIterator(new MockDataPointIterator(Arrays.asList(new DataPoint(1L, 2L), new DataPoint(3L, 4L))));
		GrafanaOutputv2 out2 = new GrafanaOutputv2("targetname2", false);

		out2.setPointsIterator(new MockDataPointIterator(Arrays.asList(new DataPoint(1L, 2L), new DataPoint(3L, 4L))));
		GrafanaOutputv2 out3 = new GrafanaOutputv2("targetname2", false);
		out3.setPointsIterator(new MockDataPointIterator(Arrays.asList(new DataPoint(1L, 2L))));
		GrafanaOutputv2 out4 = new GrafanaOutputv2("targetname2", false);
		out4.setPointsIterator(new MockDataPointIterator(Arrays.asList(new DataPoint(1L, 2L), new DataPoint(3L, 4L))));
		output = new GrafanaStreamingOutput(
				Arrays.asList(Arrays.asList(out1, out2).iterator(), Arrays.asList(out3, out4).iterator()));
		os = new ByteArrayOutputStream();
		output.write(os);
		payload = new String(os.toByteArray());
		ary = new Gson().fromJson(payload, JsonArray.class);
		assertEquals(4, ary.size());
		assertEquals(2, ary.get(0).getAsJsonObject().get("datapoints").getAsJsonArray().size());
		assertEquals(2, ary.get(1).getAsJsonObject().get("datapoints").getAsJsonArray().size());
		assertEquals(1, ary.get(2).getAsJsonObject().get("datapoints").getAsJsonArray().size());
	}

}