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
package com.srotya.sidewinder.core.predicates;

import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * Predicate is a condition or boolean operator that is used for numeric data
 * comparison right at the storage layer.
 * 
 * Predicate concept in Sidewinder is designed such that Predicates can be
 * pushed down from the query layer all the way to the storage layer to avoid
 * the necessity to convert time series data to {@link DataPoint} objects which
 * will have more overhead compared to droping primitives.
 * 
 * @author ambud
 */
public interface Predicate {

	boolean apply(long value);

}
