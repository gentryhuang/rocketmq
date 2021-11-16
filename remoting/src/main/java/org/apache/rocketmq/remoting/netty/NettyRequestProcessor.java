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
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * RocketMQ 请求处理器，如 SendMessageProcessor 是消息发送处理器、PullMessageProcessor 是消息拉取处理器
 * 说明：
 * 服务端会将客户端请求进行分类，每个命令或每类请求命令定义一个处理器(NettyRequestProcessor)，然后每一个NettyRequestProcessor绑定到
 * 一个单独的线程池，进行命令处理，不同类型的请求将使用不同的线程池进行处理，实现线程隔离。
 * Common remoting command processor
 */
public interface NettyRequestProcessor {
    /**
     * 处理请求接口
     *
     * @param ctx
     * @param request
     * @return
     * @throws Exception
     */
    RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws Exception;

    /**
     * 是否要拒绝请求
     *
     * @return
     */
    boolean rejectRequest();

}
