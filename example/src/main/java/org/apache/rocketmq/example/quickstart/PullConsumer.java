package org.apache.rocketmq.example.quickstart;

import com.sun.xml.internal.ws.api.model.wsdl.WSDLOutput;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Set;

/**
 * DefaultMQPullConsumer 使用者自主控制接收消息 示例
 */
public class PullConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("pull_consumer_test");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.start();
        Set<MessageQueue> messageQueues = consumer.fetchSubscribeMessageQueues("TopicTest");
        for (MessageQueue message : messageQueues){
            System.out.printf("%s%n",message);
            try {
                PullResult pullResult = consumer.pull(message,"*",1394,10);
                pullResult.getMsgFoundList().forEach(messageExt -> {
                    System.out.println(new String(messageExt.getBody()));
                });
            } catch (RemotingException e) {
                e.printStackTrace();
            } catch (MQBrokerException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }


    }

}
