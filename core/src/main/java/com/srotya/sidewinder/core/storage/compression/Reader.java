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
package com.srotya.sidewinder.core.storage.compression;

import java.security.NoSuchAlgorithmException;

import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.RejectException;

/**
 * @author ambud
 */
public interface Reader {

	public static final RejectException EOS_EXCEPTION = new RejectException("End of stream reached");
	public static final FilteredValueException FILTERED_VALUE_EXCEPTION = new FilteredValueException();

	public long read() throws RejectException, FilteredValueException;

	public default double readDouble() throws RejectException, FilteredValueException {
		return Double.longBitsToDouble(read());
	}

	public int getCounter();

	public int getCount();

	public void setPredicate(Predicate predicate);

	public byte[] getDataHash() throws NoSuchAlgorithmException;

}
