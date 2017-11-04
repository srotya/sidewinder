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
package com.srotya.sidewinder.ingesters.influx;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.codahale.metrics.Counter;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.srotya.sidewinder.core.monitoring.ResourceMonitor;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.InfluxDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;

/**
 * HTTP Protocol follows an InfluxDB wire format for ease of use with clients.
 * 
 * References:
 * https://netty.io/4.0/xref/io/netty/example/http/snoop/HttpSnoopServerHandler.html
 * 
 * @author ambud
 */
public class HTTPDataPointDecoder extends SimpleChannelInboundHandler<Object> {

	private static final Logger logger = Logger.getLogger(HTTPDataPointDecoder.class.getName());
	private StringBuilder responseString = new StringBuilder();
	private HttpRequest request;
	private StorageEngine engine;
	private String dbName;
	private String path;
	private StringBuilder requestBuffer = new StringBuilder();
	private Counter meter;

	public HTTPDataPointDecoder(StorageEngine engine, Counter meter) {
		this.engine = engine;
		this.meter = meter;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
		try {
			if (ResourceMonitor.getInstance().isReject()) {
				logger.warning("Write rejected, insufficient memory");
				if (writeResponse(request, ctx)) {
					ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
				}
				return;
			}
			if (msg instanceof HttpRequest) {
				HttpRequest request = this.request = (HttpRequest) msg;
				if (HttpUtil.is100ContinueExpected(request)) {
					send100Continue(ctx);
				}

				QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());
				path = queryStringDecoder.path();

				Map<String, List<String>> params = queryStringDecoder.parameters();
				if (!params.isEmpty()) {
					for (Entry<String, List<String>> p : params.entrySet()) {
						String key = p.getKey();
						if (key.equalsIgnoreCase("db")) {
							dbName = p.getValue().get(0);
						}
					}
				}

				if (path != null && path.contains("query")) {
					Gson gson = new Gson();
					JsonObject obj = new JsonObject();
					JsonArray ary = new JsonArray();
					ary.add(new JsonObject());
					obj.add("results", ary);
					responseString.append(gson.toJson(obj));
				}
			}

			if (msg instanceof HttpContent) {
				HttpContent httpContent = (HttpContent) msg;
				ByteBuf byteBuf = httpContent.content();
				if (byteBuf.isReadable()) {
					requestBuffer.append(byteBuf.toString(CharsetUtil.UTF_8));
				}

				if (msg instanceof LastHttpContent) {
					if (dbName == null) {
						responseString.append("Invalid database null");
						logger.severe("Invalid database null");
					} else {
						String payload = requestBuffer.toString();
						logger.fine("Request:" + payload);
						List<DataPoint> dps = InfluxDecoder.dataPointsFromString(dbName, payload);
						meter.inc(dps.size());
						for (DataPoint dp : dps) {
							try {
								engine.writeDataPoint(dp);
								logger.fine("Accepted:" + dp + "\t" + new Date(dp.getTimestamp()));
							} catch (IOException e) {
								logger.fine("Dropped:" + dp + "\t" + e.getMessage());
								responseString.append("Dropped:" + dp);
							}
						}
					}
					if (writeResponse(request, ctx)) {
						ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
	}

	private boolean writeResponse(HttpObject httpObject, ChannelHandlerContext ctx) {
		FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1,
				httpObject.decoderResult().isSuccess() ? OK : BAD_REQUEST,
				Unpooled.copiedBuffer(responseString.toString().toString(), CharsetUtil.UTF_8));
		response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");

		response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
		response.headers().set(CONNECTION, HttpHeaderValues.KEEP_ALIVE);

		responseString = new StringBuilder();
		// Write the response.
		ctx.write(response);
		return true;
	}

	private static void send100Continue(ChannelHandlerContext ctx) {
		FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, CONTINUE);
		ctx.write(response);
	}
}
