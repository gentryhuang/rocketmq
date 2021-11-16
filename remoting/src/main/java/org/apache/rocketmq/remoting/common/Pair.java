/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.remoting.common;

/**
 * 用来封装 NettyRequestProcessor 与 ExecuteService 的绑定关系。因为在 RocketMQ 的网络模型中，会为每一个 NettyRequestProcessor 与特定的线程池绑定，
 * 所有该 NettyRequestProcessor 的处理逻辑都在该线程池中运行。
 *
 * @param <T1>
 * @param <T2>
 */
public class Pair<T1, T2> {
    /**
     * NettyRequestProcessor
     */
    private T1 object1;
    /**
     * ExecutorService
     */
    private T2 object2;

    public Pair(T1 object1, T2 object2) {
        this.object1 = object1;
        this.object2 = object2;
    }

    public T1 getObject1() {
        return object1;
    }

    public void setObject1(T1 object1) {
        this.object1 = object1;
    }

    public T2 getObject2() {
        return object2;
    }

    public void setObject2(T2 object2) {
        this.object2 = object2;
    }
}
