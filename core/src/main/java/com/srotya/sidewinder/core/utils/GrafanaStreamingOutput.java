package com.srotya.sidewinder.core.utils;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import com.srotya.sidewinder.core.api.grafana.GrafanaOutputv2;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.DataPointIterator;

public class GrafanaStreamingOutput implements StreamingOutput {

	private List<Iterator<GrafanaOutputv2>> targets;

	public GrafanaStreamingOutput(List<Iterator<GrafanaOutputv2>> output) {
		this.targets = output;
	}

	@Override
	public void write(OutputStream os) throws IOException, WebApplicationException {
		os = new BufferedOutputStream(os, 8192);
		os.write('[');
		if (targets.size() > 0) {
			serializeEntry(os, 0);
			for (int i = 1; i < targets.size(); i++) {
				os.write(',');
				serializeEntry(os, i);
			}
		}
		os.write(']');
		os.close();
	}

	private void serializeEntry(OutputStream os, int i) throws IOException {
		Iterator<GrafanaOutputv2> itr = targets.get(i);
		if (itr.hasNext()) {
			serializeOutput(os, itr);
			boolean addComma = true;
			while (itr.hasNext()) {
				if (addComma) {
					os.write(',');
				}
				addComma = serializeOutput(os, itr);
			}
		}
	}

	private boolean serializeOutput(OutputStream os, Iterator<GrafanaOutputv2> itr) throws IOException {
		GrafanaOutputv2 gOutput = itr.next();
		if (gOutput.getPointsIterator() != null) {
			DataPointIterator pointsIterator = gOutput.getPointsIterator();
			if (pointsIterator.hasNext()) {
				os.write(("{\"target\":\"" + gOutput.getTarget() + "\",\"datapoints\":[").getBytes());
				serializePoint(os, pointsIterator, gOutput);
				while (pointsIterator.hasNext()) {
					os.write(',');
					serializePoint(os, pointsIterator, gOutput);
				}
				os.write(']');
				os.write('}');
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	private void serializePoint(OutputStream os, DataPointIterator pointsIterator, GrafanaOutputv2 gOutput)
			throws IOException {
		DataPoint num = pointsIterator.next();
		StringBuilder builder = new StringBuilder();
		builder.append("[");
		if (gOutput.isFp()) {
			builder.append(String.valueOf(num.getValue()));
		} else {
			builder.append(String.valueOf(num.getLongValue()));
		}
		builder.append(",").append(String.valueOf(num.getTimestamp())).append("]");
		os.write(builder.toString().getBytes());
	}

}