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
package com.srotya.sidewinder.core.storage.compression.byzantine;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @author ambud
 */
public class NoLock implements Lock {

	@Override
	public void lock() {
		// do nothing
	}

	@Override
	public void lockInterruptibly() throws InterruptedException {
		// do nothing
	}

	@Override
	public boolean tryLock() {
		// do nothing
		return true;
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		// do nothing
		return true;
	}

	@Override
	public void unlock() {
		// do nothing
	}

	@Override
	public Condition newCondition() {
		// do nothing
		return null;
	}

}