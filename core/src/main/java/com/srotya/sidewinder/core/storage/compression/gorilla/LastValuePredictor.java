/** 
 * 	Copyright 2016-2018 Michael Burman and/or other contributors.
*
*   Licensed under the Apache License, Version 2.0 (the "License");
*   you may not use this file except in compliance with the License.
*   You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*   Unless required by applicable law or agreed to in writing, software
*   distributed under the License is distributed on an "AS IS" BASIS,
*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*   See the License for the specific language governing permissions and
*   limitations under the License.
**/  
package com.srotya.sidewinder.core.storage.compression.gorilla;

/**
 * Last-Value predictor, a computational predictor using previous value as a prediction for the next one
 *
 * @author Michael Burman
 */
public class LastValuePredictor implements Predictor {
    private long storedVal = 0;

    public LastValuePredictor() {}

    public void update(long value) {
        this.storedVal = value;
    }

    public long predict() {
        return storedVal;
    }
}
