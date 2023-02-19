package yu.test.producer.Orderly;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import yu.test.entity.Order;

import java.util.ArrayList;
import java.util.List;

public class SyncOrderlyProducer {
    public static void main(String[] args) {
        //实例化消息的生产者producer
        DefaultMQProducer producer = new DefaultMQProducer("Group_test");
        //nameServer 地址
        producer.setNamesrvAddr("127.0.0.1:9876");
        //是否延迟发送
        producer.setSendLatencyFaultEnable(false);
        List<Order> list = new ArrayList<Order>(10);
        try {
            producer.start();

            for (int i = 0; i < list.size(); i++) {
                Message msg = new Message("default_topic", "tag", list.get(i).toString().getBytes());
                msg.setKeys("order_id_100");
                //发送消息
                SendResult rs = producer.send(msg, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        //根据订单Id 选择发送的channel
                        long orderId = (Long) arg;
                        long index = orderId % list.size();// 获取队列Id
                        return mqs.get((int) index);
                    }
                }, list.get(i).getOrderId());
                System.out.println(rs);
            }
            //关闭producer
            producer.shutdown();

        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
