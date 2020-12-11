/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.TimeUnit;

/**
 * A {@link StreamOperator} for executing {@link MapFunction MapFunctions}.
 */
@Internal
public class StreamMap<IN, OUT>
		extends AbstractUdfStreamOperator<OUT, MapFunction<IN, OUT>>
		implements OneInputStreamOperator<IN, OUT> {

	private static final long serialVersionUID = 1L;
	//EDF
	private long refTime = 0;
	private long refCpuTime = 0;

	public StreamMap(MapFunction<IN, OUT> mapper) {
		super(mapper);
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		// EDF
		final long t0 = System.currentTimeMillis();

		output.collect(element.replace(userFunction.map(element.getValue())));

		// EDF
		final long t1 = System.currentTimeMillis();
		final long executionTimeMillis = t1-t0;
		executionTimeHistogram.update(executionTimeMillis);

		if ((t1 - TimeUnit.NANOSECONDS.toMillis(refTime)) > 5*1000){
			long currTime = System.nanoTime();
			long currCpuTime = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
			cpuUsage = (double) (currCpuTime - refCpuTime) / (double) (currTime - refTime);
			refTime = currTime;
			refCpuTime = currCpuTime;
		}
	}
}
