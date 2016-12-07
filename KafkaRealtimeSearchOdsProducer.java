package com.bj58.javautils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaRealtimeSearchOdsProducer implements Serializable{
	private static final long serialVersionUID = 1L;
	private static final  List<Producer<String, String>> producerList = new ArrayList<Producer<String, String>>();
	public static Properties props = new Properties();
	public static Producer<String, String> producer = null;
	private static ProducerConfig config =null;   
	
	static{
		props.put("metadata.broker.list", "10.126.99.105:9092,10.126.99.196:9092,10.126.81.208:9092,10.126.121.194:9092,10.126.81.215:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		props.put("compression.codec", "2");
		config = new ProducerConfig(props);
	    if ((producerList.size() > 0) && (producerList.get(0) != null)) {
	      producer = producerList.get(0);
	      producerList.remove(0);
	    } else {
	      producer = new Producer<String, String>(config);
	    }  
	}
	

	public static void producerSend(String msg, String topic){
		KeyedMessage<String, String> message = new KeyedMessage<String, String>(topic, msg);
		try{
			producer.send(message);
		}catch (Exception e){
			e.printStackTrace();
			System.out.println("***");
		}
	}
	
	public static void closeProducer(Producer<String, String> producer){
		producer.close();
	}
	
	
	public static void main(String[] args) {
		String asd = "lianmengceshi lianmengceshi lianmengceshi lianmengceshi";
		for(int i=0; i< 10000; i++){
			String vs = asd + i;
			KafkaRealtimeSearchOdsProducer.producerSend(vs, "hdp_lbg_ectech_lm_dwd_detail");
		}
		KafkaRealtimeSearchOdsProducer.closeProducer(producer);
	}
}
