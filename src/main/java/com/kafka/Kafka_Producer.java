package com.kafka;

import java.util.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import scala.Tuple2;

public class Kafka_Producer {
    public static void main(String[] args) {
        Gen_data gen_data_obj = new Gen_data();
        gen_data_obj.gen_data();
    }
}

class Gen_data {
    private Random random;
    private Click_data clicks;
    private Purchase_data purchases;
    private String bootstrap_servers;
    
    private ProducerRecord<String, String> record;
    private KafkaProducer<String, String> producer;
    
    public Gen_data() {
        this.random = new Random();
        
        this.clicks = new Click_data();
        this.purchases = new Purchase_data();
        this.bootstrap_servers = "kafka:9092";
        
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrap_servers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        this.producer = new KafkaProducer<>(properties);
    }
    public void gen_data() {
        List<Tuple2<String, String>> potential_purchase_list = new ArrayList<>();
        int n = 1000;
        
        while (n > 0) {
            // n--; // For testing only
            int rand_n = Math.abs(this.random.nextInt());        
            if (rand_n % 2 == 0) {
                String click_message = this.clicks.Create_message();
                
                record = new ProducerRecord<String, String>("clicks", click_message);
                potential_purchase_list.add(new Tuple2<String,String>(this.clicks.Get_userID(), 
                                                                this.clicks.Get_productID()));

                System.out.println(click_message);    
                producer.send(record);
                
            }

            if (rand_n % 7 == 0) {
                int random_idx = random.nextInt(potential_purchase_list.size());
                Tuple2<String, String> info = potential_purchase_list.get(random_idx);
                
                String purchase_message = this.purchases.Create_message(info._1(), info._2()); 
                
                System.out.println(purchase_message);    
                record = new ProducerRecord<String, String>("purchases", purchase_message);
                producer.send(record);
            }

            // if (n == 0) { // For testing only
            //     try {
            //         Thread.sleep(10000);
            //     } catch (InterruptedException e) {
            //         e.printStackTrace();
            //     }
            //     n = 1000;
            // }
        }
    }
}   
