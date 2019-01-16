package com.srotya.sidewinder.core.api.grafana;

import java.util.Iterator;
import java.util.List;

import com.srotya.sidewinder.core.storage.DataPointIterator;
import com.srotya.sidewinder.core.storage.SeriesOutputv2;

final class GrafanaOutputv2Iterator implements Iterator<GrafanaOutputv2> {

	private Iterator<SeriesOutputv2> iterator;

	public GrafanaOutputv2Iterator(List<SeriesOutputv2> series) {
		iterator = series.iterator();
	}

	@Override
	public GrafanaOutputv2 next() {
		SeriesOutputv2 entry = iterator.next();
		GrafanaOutputv2 tar = new GrafanaOutputv2(entry.toString(), entry.isFp());
		DataPointIterator iterator = entry.getIterator();
		if (iterator != null) {
			tar.setPointsIterator(iterator);
		}
		return tar;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}
}