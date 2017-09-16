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
package com.srotya.sidewinder.cluster.push.routing;

import java.io.IOException;

import com.srotya.sidewinder.core.rpc.Point;

/**
 * Writer is an abstraction to allow for data points to be either written
 * locally or to be sent to another Sidewinder node over network.
 * 
 * Writer is encapsulated within a {@link Node} to have the need to avoid
 * searching for transport mechanism when {@link RoutingStrategy} generates a
 * list of placement nodes where a given datapoint should be sent.
 * 
 * @author ambud
 */
public interface EndpointService {

	public void write(Point point) throws IOException;
	
	public void requestRouteEntry(Point point) throws IOException;

	public void close() throws IOException;

}
