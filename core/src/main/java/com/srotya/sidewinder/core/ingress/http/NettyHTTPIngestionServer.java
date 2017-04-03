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
package com.srotya.sidewinder.core.ingress.http;

import java.util.Map;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * A Netty based HTTP ingress server implementation for Sidewinder
 * 
 * @author ambud
 */
public class NettyHTTPIngestionServer {

	private Channel channel;
	private StorageEngine storageEngine;
	private Meter meter;

	public void init(StorageEngine storageEngine, Map<String, String> conf, MetricRegistry registry) {
		this.storageEngine = storageEngine;
		meter = registry.meter("writes");
	}

	public void start() throws InterruptedException {
		EventLoopGroup bossGroup = new NioEventLoopGroup(1, new BackgrounThreadFactory("httpBossGroup"));
		EventLoopGroup workerGroup = new NioEventLoopGroup(1, new BackgrounThreadFactory("httpWorkerGroup"));
		EventLoopGroup processorGroup = new NioEventLoopGroup(2, new BackgrounThreadFactory("httpProcessorGroup"));

		ServerBootstrap bs = new ServerBootstrap();
		channel = bs.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
				.option(ChannelOption.SO_RCVBUF, 10485760).option(ChannelOption.SO_SNDBUF, 10485760)
				.handler(new LoggingHandler(LogLevel.DEBUG)).childHandler(new ChannelInitializer<SocketChannel>() {

					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						ChannelPipeline p = ch.pipeline();
						p.addLast(processorGroup, new HttpRequestDecoder());
						p.addLast(processorGroup, new HttpResponseEncoder());
						p.addLast(processorGroup, new HTTPDataPointDecoder(storageEngine, meter));
					}

				}).bind("localhost", 9928).sync().channel();
	}

	public void stop() throws InterruptedException {
		channel.closeFuture().await();
	}

}
