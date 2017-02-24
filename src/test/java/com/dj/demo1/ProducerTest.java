package com.dj.demo1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.Random;

/**
 * 单元测试类
 *
 * Created by Leo on 2017-2-21.
 */
public class ProducerTest {


    public static  final Logger log = Logger.getLogger(ProducerTest.class);


    /**
     *
     * produce demo1:
     *              采用默认轮询发送到不同分区
     *              不需要回调
     * @throws InterruptedException
     */
    @Test
    public void produce() throws InterruptedException {
        Producer<String,String> producer = KafkaProducerFactory.getProducer();

        String topic  = "topic201702241031";
        String data = " time:"+ System.currentTimeMillis();


        int i = 0;

        while (true){
            try {
                //发送的Topic的KeyKey(可以没有 如果有 会根据key哈希到不同的分区)/Value
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, data);
                producer.send(record);
                log.info("-------send  topic:"+topic+"  success--------data:"+data);

            }catch (Exception e){
                log.error("------send topic"+topic+" error!---data:"+data);
                log.error(e);
                //根据业务判断
                //1. 出错是自动跳过 继续运行(produce不close)


                //2. 直接break down 停止运行 (producer.close)
                //producer.close();
                //return;
            }
           i++;
            if(i > 100){
                break;
            }
        }






    }



    @Test
    public void produceCallback() throws InterruptedException {
        Producer<String,String> producer = KafkaProducerFactory.getProducer();

        int i = 1000;
        String topic  = "test";
        long start = System.currentTimeMillis();
        String data = "topic:test time:"+ System.currentTimeMillis();
        try {
                while(true){

                        //发送的Topic的Key(可以没有 如果有 会根据key哈希到不同的分区)/Value
                        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, data);
                        //注意，如果acks配置为0，依然会触发回调逻辑，只是拿不到 offset和消息落地的分区信息。
                        producer.send(record, new Callback() {
                            public void onCompletion(RecordMetadata metadata, Exception exception) {
                                if(exception == null){
                                    log.info("topic send callback success !! ---topic:"+metadata.topic()+"---   partition:"+metadata.partition()+"---  offset:"+metadata.offset());
                                }else {
                                    log.info("topic callback error！"+exception);
                                }
                            }
                        });
                        log.info("-------send  topic:"+topic+"  success--------data:"+data);
                    i++;
                    if(i == 2000){
                        log.info("----send 1000 cost "+(System.currentTimeMillis()-start));
                        producer.close();
                        return ;
                    }
                }
            }catch (Exception e){
                 Thread.sleep(3000);
                log.error("------send topic"+topic+" error!---data:"+data);
                log.error(e);
                try {
                    producer.close();
                }catch ( Exception e1){
                    log.error(e);
                }
            }


//            Thread.sleep(1000);

    }




    @Test
    public void produceByKeyHash() throws InterruptedException {
        Producer<String,String> producer = KafkaProducerFactory.getProducer();

        String topic  = "topic201702331414";
        String data = " time:"+ System.currentTimeMillis();



        while (true){
            try {
                String key = String.valueOf(new Random().nextInt());
                //发送的Topic的Key(可以没有 如果有 会根据key哈希到不同的分区)/Value
                //可以通过设置 partitioner.class 指定发送分区的算法
                // 默认没有key 采用轮询算法;有key 没有指定partitioner.class,采用key的Hash算法

                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key,data);
                //注意，如果acks配置为0，依然会触发回调逻辑，只是拿不到 offset和消息落地的分区信息。
                producer.send(record,new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception == null){
                            log.info("topic send callback success !! ---topic:"+metadata.topic()+"---   partition:"+metadata.partition()+"---  offset:"+metadata.offset());
                        }else {
                            log.info("topic callback error！"+exception);
                        }
                    }
                });
                log.info("-------send  topic:"+topic+"  success-----key:"+key+"---data:"+data);
            }catch (Exception e){
                log.error("------send topic"+topic+" error!---data:"+data);
                log.error(e);
            }


            //Thread.sleep(1000);
        }

    }



}
