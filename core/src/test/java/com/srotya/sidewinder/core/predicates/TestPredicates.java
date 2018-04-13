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
package com.srotya.sidewinder.core.predicates;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

/**
 * @author ambud
 */
public class TestPredicates {

	@Test
	public void testAndPredicate() {
		long ts = System.currentTimeMillis();
		ComplexPredicate predicate = new AndPredicate(Arrays.asList(new EqualsPredicate(ts)));
		assertTrue(predicate.test(ts));
		predicate = new AndPredicate(
				Arrays.asList(new LessThanEqualsPredicate(ts + 1), new GreaterThanEqualsPredicate(ts - 1)));
		assertTrue(predicate.test(ts));
		assertTrue(!predicate.test(ts + 4));
	}

	@Test
	public void testOrPredicate() {
		long ts = System.currentTimeMillis();
		Predicate predicate = new OrPredicate(Arrays.asList(new EqualsPredicate(ts)));
		assertTrue(predicate.test(ts));
		predicate = new OrPredicate(
				Arrays.asList(new LessThanEqualsPredicate(ts + 1), new GreaterThanEqualsPredicate(ts - 1)));
		assertTrue(predicate.test(ts));
		assertTrue(predicate.test(ts + 4));
	}

	@Test
	public void testNotPredicate() {
		long ts = System.currentTimeMillis();
		Predicate predicate = new NotPredicate(Arrays.asList(new EqualsPredicate(ts)));
		assertTrue(!predicate.test(ts));
		try {
			new NotPredicate(Arrays.asList(new EqualsPredicate(ts), new EqualsPredicate(ts + 1)));
			fail("Invalid argument");
		} catch (IllegalArgumentException e) {
		}
	}

	@Test
	public void testEqualsPredicate() {
		long ts = System.currentTimeMillis();
		Predicate predicate = new EqualsPredicate(ts);
		assertTrue(predicate.test(ts));
	}

	@Test
	public void testBetweenPredicate() {
		long ts = System.currentTimeMillis();
		Predicate predicate = new BetweenPredicate(ts - 1, ts + 1);
		assertTrue(predicate.test(ts));
		assertTrue(!predicate.test(ts + 2));
	}

	@Test
	public void testGreaterThanEqualsPredicate() {
		long ts = System.currentTimeMillis();
		Predicate predicate = new GreaterThanEqualsPredicate(ts - 1);
		assertTrue(predicate.test(ts));
		assertTrue(predicate.test(ts - 1));
		assertTrue(!predicate.test(ts - 2));
	}

	@Test
	public void testGreaterThanPredicate() {
		long ts = System.currentTimeMillis();
		Predicate predicate = new GreaterThanPredicate(ts - 1);
		assertTrue(predicate.test(ts));
		assertTrue(!predicate.test(ts - 1));
		assertTrue(!predicate.test(ts - 2));
	}

	@Test
	public void testLessThanPredicate() {
		long ts = System.currentTimeMillis();
		Predicate predicate = new LessThanPredicate(ts);
		assertTrue(!predicate.test(ts));
		assertTrue(predicate.test(ts - 1));
		assertTrue(predicate.test(ts - 2));
	}

	@Test
	public void testLessThanEqualsPredicate() {
		long ts = System.currentTimeMillis();
		Predicate predicate = new LessThanEqualsPredicate(ts);
		assertTrue(!predicate.test(ts + 1));
		assertTrue(predicate.test(ts));
		assertTrue(predicate.test(ts - 1));
		assertTrue(predicate.test(ts - 2));
	}
}
