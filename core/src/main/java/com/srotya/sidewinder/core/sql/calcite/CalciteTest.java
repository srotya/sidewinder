package com.srotya.sidewinder.core.sql.calcite;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.srotya.sidewinder.core.api.SqlApi;
import com.srotya.sidewinder.core.storage.TimeSeries;
import com.srotya.sidewinder.core.storage.mem.MemStorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;

public class CalciteTest {

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		ScheduledExecutorService bgt = Executors.newScheduledThreadPool(1,
				new BackgrounThreadFactory("sidewinderbg-tasks"));
		MemStorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), bgt);
		SqlApi api = new SqlApi(engine);
		api.initCalcite();

		long ts = System.currentTimeMillis();
		TimeSeries s = engine.getOrCreateTimeSeries("db1", "m1", "v1", Arrays.asList("t=1", "p=2"), 1024, false);
		for(int i=0;i<100;i++) {
			s.addDataPoint(TimeUnit.MILLISECONDS, ts+i*1000, i);
		}
		
		String queryResults = api.queryResults("db1", "select * from db1.m1 where time_stamp>1519945488603");
		System.out.println(queryResults);
	}
}
