package cn.likegirl.rocketmp.simple;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

public class PullConsumer {
	private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

	public static void main(String[] args) throws MQClientException {
		DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("please_rename_unique_group_name_5");

		/**
		 * 配置： brokerSuspendMaxTimeMillis 20000
		 * 长轮询，consumer拉消息请求在broker挂起最长时间，单位毫秒 consumerPullTimeoutMillis 10000
		 * 非长轮询，拉消息超时时间，单位毫秒 consumerTimeoutMillisWhenSuspend 30000
		 * 长轮询，consumer拉消息请求，broker挂起超过指定时间，客户端认为超时，单位毫秒 messageModel
		 * MessageModel.BROADCASTING 消息模型，支持以下两种：1.集群消费 2.广播模式
		 * messageQueueListener 监听队列变化 offsetStore 消费进度存储 registerTopics
		 * 注册topic集合
		 *
		 */
		consumer.start();

		Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest1");
		for (MessageQueue mq : mqs) {
			System.out.printf("Consume from the queue: %s%n", mq);
			SINGLE_MQ: while (true) {
				try {
					PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
					System.out.printf("%s%n", pullResult);
					putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
					switch (pullResult.getPullStatus()) {
					case FOUND:
						List<MessageExt> list = pullResult.getMsgFoundList();
						for (MessageExt msg : list) {
							System.out.println(new String(msg.getBody()));
						}
						break;
					case NO_MATCHED_MSG:
						break;
					case NO_NEW_MSG:
						break SINGLE_MQ;
					case OFFSET_ILLEGAL:
						break;
					default:
						break;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		consumer.shutdown();
	}

	private static long getMessageQueueOffset(MessageQueue mq) {
		Long offset = OFFSE_TABLE.get(mq);
		if (offset != null)
			return offset;

		return 0;
	}

	private static void putMessageQueueOffset(MessageQueue mq, long offset) {
		OFFSE_TABLE.put(mq, offset);
	}

}
