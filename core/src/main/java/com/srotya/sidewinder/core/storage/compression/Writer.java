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

import java.io.IOException;

import javax.validation.constraints.NotNull;

import com.srotya.sidewinder.core.storage.Buffer;
import com.srotya.sidewinder.core.storage.LinkedByteString;
import com.srotya.sidewinder.core.storage.RejectException;

/**
 * @author ambud
 */
public interface Writer {

	public static final RollOverException BUF_ROLLOVER_EXCEPTION = new RollOverException();
	public static final RejectException WRITE_REJECT_EXCEPTION = new RejectException();

	public void add(long value) throws IOException;

	public default void write(long value) throws IOException {
		add(value);
	}

	public Reader getReader() throws IOException;

	public double getCompressionRatio();

	public void configure(Buffer buf, boolean isNew, int startOffset) throws IOException;

	public void bootstrap(Buffer buf) throws IOException;

	public Buffer getRawBytes();

	public void setCounter(int counter);

	public void makeReadOnly(boolean recovery) throws IOException;

	public int currentOffset();

	public int getCount();

	public boolean isFull();

	public boolean isReadOnly();

	public int getPosition();

	public void setBufferId(LinkedByteString key);

	@NotNull
	public LinkedByteString getBufferId();

}