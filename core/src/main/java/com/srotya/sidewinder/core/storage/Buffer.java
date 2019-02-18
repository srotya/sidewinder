package com.srotya.sidewinder.core.storage;

public interface Buffer {

	void put(int offset, byte b);

	int remaining();

	void get(byte[] buf);

	void position(int startOffset);

	void putInt(int i);

	int getInt();

	long getLong();

	byte get();

	int getShort();

	Buffer duplicate();

	void putShort(short xor);

	void putLong(long xor);

	boolean isReadOnly();

	void rewind();

	void putInt(int index, int value);

	int position();

	int limit();

	void put(Buffer buf);

	int capacity();

	byte[] array();

	byte get(int position);

	boolean hasRemaining();

	void put(byte[] b);

	long getLong(int position);
	
	Buffer newInstance(int capacity);

	Buffer slice();

	void put(byte b);

	void limit(int position);

	void flip();

}