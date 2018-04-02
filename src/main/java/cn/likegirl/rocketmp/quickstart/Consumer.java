package cn.likegirl.rocketmp.quickstart;

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

import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragelyByCircle;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 */
public class Consumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {

        /*
         * Instantiate with specified consumer group name.
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("quickstart_consumer");
        
        /*
         * messageModel MessageModel.CLUSTERING 消息模型，支持一下两种 1.集群消费 2.广播消费
         * consumeFromWhere ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET consumer启动后，默认从什么位置开始消费
         * allocateMessageQueueStrategy
         * subscription {} 订阅关系
         * messageListener 消息监听器
         * offsetStore 消息进度储存
         * consumeThreadMin 10 消费线程池数量
         * consumeThreadMax 20 消费线程池数量
         * consumeConcurrentlyMaxSpan 2000 单队列并行消费允许的最大跨度
         * pullThresholdForQueue 1000 拉消息本地队列缓存消息最大数
         * pullInterval 拉消息间隔，由于是长轮询，所以为0，但是如果应用了流控，也可以设置大于0的值，单位毫秒
         * consumeMessageBatchMaxSize 1 批量消息，一次消费对少条消息
         * pullBatchSize 32 批量拉消息，一次最多拉多少条
         * 
         */
        
//        AllocateMessageQueueAveragelyByCircle cricle = new AllocateMessageQueueAveragelyByCircle();
//        AllocateMessageQueueAveragely avg = new AllocateMessageQueueAveragely();
//        consumer.setAllocateMessageQueueStrategy(cricle);
        
        
        /*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * consumer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */

        /*
         * Specify where to start in case the specified consumer group is a brand new one.
         * 指定从哪里开始，以防止指定的消费群体是全新的。
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeThreadMin(10);
        consumer.setConsumeThreadMax(50);
        
        consumer.setNamesrvAddr("192.168.179.31:9876;192.168.179.32:9876");
//        consumer.setNamesrvAddr("193.112.133.164:9876;114.67.226.113:9876");
        /*
         * Subscribe one more more topics to consume.
         */
        consumer.subscribe("TopicQuickStart", "*");

        /*
         *  Register callback to execute on arrival of messages fetched from brokers.
         */
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
            	MessageExt msg = msgs.get(0);
            	try {
					System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msg);
//                  String orignMsgId = msg.getProperties().get(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
				} catch (Exception e) {
					e.printStackTrace();
					if(msg.getReconsumeTimes() == 3){
						System.err.println("----------记录日志---------");
						return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
					}
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				}
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        /*
         *  Launch the consumer instance.
         */
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
