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
package com.srotya.sidewinder.core.functions;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import com.srotya.sidewinder.core.functions.transform.BasicTransformFunctions.*;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.Series;

public class TestTransformFunctions {
	
	@Test
	public void testCubeRoot() {
		Function f = new CbrtFunction();
		List<Series> series = new ArrayList<>();
		Series s = new Series(Arrays.asList(new DataPoint(1L, 27), new DataPoint(1L, 64)));
		series.add(s);
		List<Series> apply = f.apply(series);
		assertEquals(2, apply.get(0).getDataPoints().size());
		assertEquals(3, apply.get(0).getDataPoints().get(0).getLongValue());
		assertEquals(4, apply.get(0).getDataPoints().get(1).getLongValue());
	}
	
	@Test
	public void testSquareRoot() {
		Function f = new SqrtFunction();
		List<Series> series = new ArrayList<>();
		Series s = new Series(Arrays.asList(new DataPoint(1L, 9), new DataPoint(1L, 16)));
		series.add(s);
		List<Series> apply = f.apply(series);
		assertEquals(2, apply.get(0).getDataPoints().size());
		assertEquals(3, apply.get(0).getDataPoints().get(0).getLongValue());
		assertEquals(4, apply.get(0).getDataPoints().get(1).getLongValue());
	}
	
	@Test
	public void testSquare() {
		Function f = new SquareFunction();
		List<Series> series = new ArrayList<>();
		Series s = new Series(Arrays.asList(new DataPoint(1L, 3), new DataPoint(1L, 4)));
		series.add(s);
		List<Series> apply = f.apply(series);
		assertEquals(2, apply.get(0).getDataPoints().size());
		assertEquals(9, apply.get(0).getDataPoints().get(0).getLongValue());
		assertEquals(16, apply.get(0).getDataPoints().get(1).getLongValue());
	}
	
	@Test
	public void testCube() {
		Function f = new CubeFunction();
		List<Series> series = new ArrayList<>();
		Series s = new Series(Arrays.asList(new DataPoint(1L, 3), new DataPoint(1L, 4)));
		series.add(s);
		List<Series> apply = f.apply(series);
		assertEquals(2, apply.get(0).getDataPoints().size());
		assertEquals(27, apply.get(0).getDataPoints().get(0).getLongValue());
		assertEquals(64, apply.get(0).getDataPoints().get(1).getLongValue());
	}
	
	@Test
	public void testCeil() {
		Function f = new CeilFunction();
		List<Series> series = new ArrayList<>();
		Series s = new Series(Arrays.asList(new DataPoint(1L, 3.2), new DataPoint(1L, 4.7)));
		series.add(s);
		List<Series> apply = f.apply(series);
		assertEquals(2, apply.get(0).getDataPoints().size());
		assertEquals(3.2, apply.get(0).getDataPoints().get(0).getValue(), 0.1);
		assertEquals(4.7, apply.get(0).getDataPoints().get(1).getValue(), 0.1);
	}
	
	@Test
	public void testLog10() {
		Function f = new Log10Function();
		List<Series> series = new ArrayList<>();
		Series s = new Series(Arrays.asList(new DataPoint(1L, 100), new DataPoint(1L, 1000)));
		series.add(s);
		List<Series> apply = f.apply(series);
		assertEquals(2, apply.get(0).getDataPoints().size());
		assertEquals(2, apply.get(0).getDataPoints().get(0).getLongValue(), 0.1);
		assertEquals(3, apply.get(0).getDataPoints().get(1).getLongValue(), 0.1);
	}
	
	@Test
	public void testLog() {
		Function f = new LogFunction();
		List<Series> series = new ArrayList<>();
		Series s = new Series(Arrays.asList(new DataPoint(1L, 100), new DataPoint(1L, 1000)));
		series.add(s);
		List<Series> apply = f.apply(series);
		assertEquals(2, apply.get(0).getDataPoints().size());
		assertEquals(4.0, apply.get(0).getDataPoints().get(0).getLongValue(), 0.1);
		assertEquals(6.0, apply.get(0).getDataPoints().get(1).getLongValue(), 0.1);
	}
}
