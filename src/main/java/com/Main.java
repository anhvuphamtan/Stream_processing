package com;

import java.io.IOException;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args)  {
        ExecutorService executor = Executors.newFixedThreadPool(4); 
        try {
            com.postgres.Init_db.main(args);
        } catch (IOException e) {    
            e.printStackTrace();
        } 
        
        executor.execute(() -> com.kafka.Kafka_Producer.main(args));
       
        
        executor.execute(() -> {
            try {
                com.stream_processing.Get_min_click.main(args);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        
        executor.execute(() -> com.stream_processing.Checkouts.main(args));
        
        
        executor.execute(() -> com.stream_processing.Stream_cal_final_result.main(args));
    }
}
