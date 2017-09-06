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
package com.srotya.sidewinder.core.graphite;

import java.util.Map;

import com.srotya.sidewinder.core.storage.StorageEngine;

import io.dropwizard.lifecycle.Managed;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * @author ambud
 */
public class GraphiteServer implements Managed {

	private StorageEngine storageEngine;
	private int serverPort;
	private Channel channel;
	private String dbName;
	private EventLoopGroup bossGroup;
	private EventLoopGroup workerGroup;

	public GraphiteServer(Map<String, String> conf, StorageEngine storageEngine) {
		this.storageEngine = storageEngine;
		this.serverPort = Integer.parseInt(conf.getOrDefault("server.graphite.port", "8772"));
		this.dbName = conf.getOrDefault("server.graphite.dbname", "graphite");
	}

	@Override
	public void start() throws Exception {
		bossGroup = new NioEventLoopGroup(1);
		workerGroup = new NioEventLoopGroup(2);

		ServerBootstrap bs = new ServerBootstrap();
		channel = bs.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
				.option(ChannelOption.SO_RCVBUF, 10485760).handler(new LoggingHandler(LogLevel.DEBUG))
				.childHandler(new ChannelInitializer<SocketChannel>() {

					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						ChannelPipeline p = ch.pipeline();
						p.addLast(workerGroup, new LineBasedFrameDecoder(1024, true, true));
						p.addLast(workerGroup, new StringDecoder());
						p.addLast(workerGroup, new GraphiteDecoder(dbName, storageEngine));
					}

				}).bind("localhost", serverPort).sync().channel();
	}

	@Override
	public void stop() throws Exception {
		workerGroup.shutdownGracefully().sync();
		bossGroup.shutdownGracefully().sync();
		channel.closeFuture().sync();
	}

}