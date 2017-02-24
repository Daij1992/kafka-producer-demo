package com.dj.demo1;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 *
 * Kafka 生产者工厂类
 * Kafka Producer采用新版的Producer 对应版本为0.8.x
 * Kafka的Producer自身是线程安全的
 * 处理逻辑是先按一定策略写入缓存,再根据配置策略多线程并发的发送到broker
 *
 * 配置采用新版的Producer配置:
 *   文档 ：http://kafka.apache.org/082/documentation.html#newproducerconfigs
 *   javaDoc: org.apache.kafka.clients.producer.ProducerConfig
 * Created by Leo on 2017-2-22.
 */
public class KafkaProducerFactory {



    private static  KafkaProducer<String,String> producer = null;

    public static KafkaProducer<String,String>  getProducer(){

        if(producer == null){
            synchronized (KafkaProducerFactory.class){
                Properties props = new Properties();
                //Kafka集群连接串，可以由多个host:port组成,指定多台机器只是为了fail-over使用
                //生产推荐至少3台的连接信息
                props.put("bootstrap.servers", "172.20.4.233:9092");
                //0：不进行消息接收确认，即Client端发送完成后不会等待Broker的确认。
                //   有最高的吞吐率，但是不保证消息是否真的发送成功(写入缓存,可能还未发到broker)
                //1：由Leader确认，Leader接收到消息后会立即返回确认信息。
                //   如果此时leader副本应答请求之后挂掉了，消息会丢失。这是个这种的方案，提供了不错的持久性保证和吞吐。
                //all：集群完整确认，Leader会等待所有in-sync的follower节点都确认收到消息后，再返回确认信息
                //   最高的消息持久性保证，但是理论上吞吐率也是最差的。
                //我们可以根据消息的重要程度，设置不同的确认模式。默认为1
                props.put("acks", "1");
                //发送失败时Producer端的重试次数，默认为0
                props.put("retries", 0);
                //当同时有大量消息要向同一个分区发送时，Producer端会将消息打包后进行批量发送。
                // 如果设置为0，则每条消息都独立发送。默认为16384字节
                props.put("batch.size", 16384);
                //送消息前等待的毫秒数，与batch.size配合使用。
                //在消息负载不高的情况下，配置linger.ms能够让Producer在发送消息前等待一定时间，以积累更多的消息打包发送，达到节省网络资源的目的。
                //默认为0
                props.put("linger.ms",0);
                //消息key/value的序列器Class，根据key和value的类型决定
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                //用来追踪定位
                props.put("client.id","producer_old_demo_127.0.0.1");


                producer = new KafkaProducer<String, String>(props);
            }
        }

        return producer;

    }

}
