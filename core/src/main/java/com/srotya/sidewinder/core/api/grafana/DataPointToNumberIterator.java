package com.srotya.sidewinder.core.api.grafana;

import java.util.Iterator;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.DataPointIterator;

public class DataPointToNumberIterator implements Iterator<Number[]> {

	private DataPointIterator iterator;
	private boolean isFp;
	private Number[] num = new Number[2];

	public DataPointToNumberIterator(DataPointIterator iterator, boolean isFp) {
		this.iterator = iterator;
		this.isFp = isFp;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public Number[] next() {
		DataPoint dp = iterator.next();
		if (isFp) {
			num[0] = dp.getValue();
		} else {
			num[0] = dp.getLongValue();
		}
		num[1] = dp.getTimestamp();
		return num;
	}

}
