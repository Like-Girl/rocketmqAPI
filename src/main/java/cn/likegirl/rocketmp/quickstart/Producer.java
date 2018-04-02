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

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * This class demonstrates how to send messages to brokers using provided {@link DefaultMQProducer}.
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {

        /*
         * Instantiate with a producer group name.
         */
        DefaultMQProducer producer = new DefaultMQProducer("quickstart_producer");
        	
        
        
        /*
         * producerGroup DEFAULT_PRODUCER
         * producerGroup Producer组名，多个producer如果属于一个应用，发送同样的消息，则应该将它们归为同一组
         * defaultTopicQueueNums 4 在发送消息时。自动创建服务器不存在的topic，默认创建的队列数
         * sendMsgTimeout 10000 发送消息超时时间，单位毫秒
         * compressMsgBodyOverHowmuch 4096 消息body超过多大开始压缩（consumer收到消息会自动解压缩），单位字节
         * retryTimesWhenSendFailed 重试次数
         * retryAnotherBrokerWhenNotStoreOK flase 如果发送消息返回sendResult，但是sendStatus!=SEND_OK，是否重试发送
         * maxMessageSize 131072 客户端限制的消息大小，超过报错，同时服务端会限制（默认128k）
         * 
         */
        producer.setRetryTimesWhenSendFailed(10);
        /*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * producer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */
        producer.setVipChannelEnabled(false);
        producer.setNamesrvAddr("192.168.179.31:9876;192.168.179.32:9876");
//        producer.setNamesrvAddr("193.112.133.164:9876;114.67.226.113:9876");
        /*
         * Launch the instance.
         */
        producer.start();

        for (int i = 0; i < 10; i++) {
            try {

                /*
                 * Create a message instance, specifying topic, tag and message body.
                 */
                Message msg = new Message("TopicQuickStart" /* Topic */,
                    "TagA" /* Tag */,
                    "key" + i /* key */,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );

                /*
                 * Call send message to deliver message to one of brokers.
                 */
                // 同步
                SendResult sendResult = producer.send(msg);
                
                // 异步
               /* producer.send(msg, new SendCallback() {
					
					@Override
					public void onSuccess(SendResult sendResult) {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public void onException(Throwable e) {
						// TODO Auto-generated method stub
						
					}
				}, 10000);*/
                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        /*
         * Shut down once the producer instance is not longer in use.
         */
        producer.shutdown();
    }
}
