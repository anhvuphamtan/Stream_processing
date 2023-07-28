package com.kafka;

import java.time.LocalDateTime;
import java.util.*;

import com.github.javafaker.Faker;

import org.apache.spark.sql.*;

class Spark_reader {
    private SparkSession spark;
    private Dataset<Row> df;
        
    public Spark_reader() {
        this.spark = SparkSession.builder()
                        .appName("dataframe_reader")
                        .master("local")
                        .getOrCreate();
    }

    public void Read_csv(String filePath) {
        this.df = this.spark.read()
                        .option("header", true)
                        .csv(filePath);
    }

    public List<Row> Get_col(String colName) {
        return df.select(colName).collectAsList();
    }

    public void Close() {
        spark.close();
    }
}

class Click_data {
    // click_id, user_id, product_id, click_time, click_at (products, homepage), source, ip_address
    private String[] arr = new String[6];   // array of click data information

    private Spark_reader spark_reader;
    private List<Row> UserID_list;
    private List<Row> ProductID_list;
    
    private Random random = new Random();
    private Faker faker = new Faker();
    private int cnt = 1;

    public Click_data() {
        this.spark_reader = new Spark_reader();
        spark_reader.Read_csv("./Input_data/Users.csv");
        this.UserID_list = spark_reader.Get_col("User_id");
        
        spark_reader.Read_csv("./Input_data/Products.csv");
        this.ProductID_list = spark_reader.Get_col("Product_ID");
    }

    private Object Get_random_val(int type) {
        int random_idx = -1;
        if (type == 1) {
            random_idx = random.nextInt(this.UserID_list.size());
            return this.UserID_list.get(random_idx).getString(0);
        }

        else {
            random_idx = random.nextInt(this.ProductID_list.size());
            return this.ProductID_list.get(random_idx).getString(0);
        }
    }

    public Object[] Gen_click_data() {
        Object[] value = new Object[6];
        value[0] = cnt++;                    // Click_ID
        value[1] = Get_random_val(1);       // User_ID
        
        value[2] = LocalDateTime.now();          // Click time
        value[3] = (random.nextInt(2) % 2 == 0) ? "Homepage" : "Product";    // User click at what
        

        String[] Sources = {"Unknown", "Facebook", "Unknown", "Twitter", "Tiktok", "Gmail", "Unknown", 
                            "Google", "Unknown", "Youtube", "Unknown", "Tiktok", "Facebook", "Google"};

        value[4] = Sources[random.nextInt(Sources.length)]; // Source before click (unknown refer to non ad click)
        value[5] = faker.internet().ipV4Address();          // IP Address

        return value;
    }

    public String Create_message() {
        Object[] value = Gen_click_data();
        String[] col_names = new String[] {"Click_ID", "User_ID", "Click_time", 
                                            "Click_at", "Source", "IP"};
        
        for (int i = 0; i < this.arr.length; i++) {
            this.arr[i] = "\"" + col_names[i] + "\":\"" + value[i].toString() + "\"";      
        }
        
        String message = "{" + String.join(",", this.arr) + "}";
        return message;
    }

    public String Get_userID() {
        return this.arr[1];
    }

    public String Get_productID() {
        return Get_random_val(2).toString();
    }
}
