package com.srotya.sidewinder.core.storage.buffer;

import java.nio.ByteBuffer;

import com.srotya.sidewinder.core.storage.Buffer;

public class GenericBuffer implements Buffer {

	public static GenericBuffer allocate(byte[] bytes) {
		return new GenericBuffer(ByteBuffer.wrap(bytes));
	}

	public static GenericBuffer allocate(int capacity) {
		return new GenericBuffer(ByteBuffer.allocate(capacity));
	}

	public static GenericBuffer allocateDirect(int capacity) {
		return new GenericBuffer(ByteBuffer.allocateDirect(capacity));
	}

	private ByteBuffer buffer;

	public GenericBuffer(ByteBuffer existingBuffer) {
		buffer = existingBuffer;
	}

	@Override
	public byte[] array() {
		return buffer.array();
	}

	@Override
	public int capacity() {
		return buffer.capacity();
	}

	@Override
	public Buffer duplicate(boolean readOnly) {
		return new GenericBuffer(this.buffer.duplicate());
	}

	@Override
	public void flip() {
		buffer.flip();
	}

	@Override
	public byte get() {
		return buffer.get();
	}

	@Override
	public void get(byte[] buf) {
		buffer.get(buf);
	}

	@Override
	public byte get(int position) {
		return buffer.get(position);
	}

	@Override
	public int getInt() {
		return buffer.getInt();
	}

	@Override
	public long getLong() {
		return buffer.getLong();
	}

	@Override
	public long getLong(int position) {
		return buffer.getLong(position);
	}

	@Override
	public int getShort() {
		return buffer.getShort();
	}

	@Override
	public boolean hasRemaining() {
		return buffer.hasRemaining();
	}

	@Override
	public boolean isReadOnly() {
		return buffer.isReadOnly();
	}

	@Override
	public int limit() {
		return buffer.limit();
	}

	@Override
	public void limit(int position) {
		buffer.limit(position);
	}

	@Override
	public Buffer newInstance(int capacity) {
		return GenericBuffer.allocate(capacity);
	}

	@Override
	public int position() {
		return buffer.position();
	}

	@Override
	public void position(int offset) {
		buffer.position(offset);
	}

	@Override
	public void put(Buffer buf) {
		for (int i = buf.position(); i < buf.limit(); i++) {
			buffer.put(buf.get(i));
		}
	}

	@Override
	public void put(byte b) {
		buffer.put(b);
	}

	@Override
	public void put(byte[] b) {
		buffer.put(b);
	}

	@Override
	public void put(int offset, byte b) {
		buffer.put(offset, b);
	}

	@Override
	public void putInt(int value) {
		buffer.putInt(value);
	}

	@Override
	public void putInt(int index, int value) {
		buffer.putInt(index, value);
	}

	@Override
	public void putLong(long value) {
		buffer.putLong(value);
	}

	@Override
	public void putShort(short value) {
		buffer.putShort(value);
	}

	@Override
	public int remaining() {
		return buffer.remaining();
	}

	@Override
	public void rewind() {
		buffer.rewind();
	}

	@Override
	public Buffer slice() {
		return new GenericBuffer(buffer.slice());
	}

}