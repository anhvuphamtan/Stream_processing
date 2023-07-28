package com.kafka;

import java.util.Random;
import java.time.LocalDateTime;

public class Purchase_data {
    private String[] arr = new String[11]; // array of purchase data information
    private Random random = new Random();
    private int purchase_ID = 1;
    
    public Object[] Gen_purchase_data(String User_ID, String Product_ID) {
        Object[] value = new Object[11];
        
        value[0] = purchase_ID++;               
        value[1] = User_ID;                 
        value[2] = Product_ID;                  
       
        value[3] = 2 + Math.round(random.nextFloat() * 100) / 100f * 18;        // Shipping_cost range [2, 20] USD
        value[4] = random.nextInt(4) * 0.05; // Discount
        value[5] = 5 + Math.round(random.nextFloat() * 100) / 100f * random.nextInt(200);               // Total_cost
        

        String[] payment_arr = {"Visa", "MasterCard", "Stripe", "Payment on Delivery", 
                                "Paypal", "Alipay", "Skrill"};
        value[6] = payment_arr[random.nextInt(payment_arr.length)];              // Payment_method
        value[7] = 1 + random.nextInt(10);                 // Quantity

        value[8] = ((1 + random.nextInt(100)) % 7 != 0)? "success" : "failure";           // Payment_status
        String[] failure_arr = {"Insufficient Fund", "Invalid Payment Details", "Network Connectivity Issues",
                                "Payment Gateway Issues", "Incorrect Billing Address", "Card Expired"};
        
        value[9] = (value[8] == "failure") ? failure_arr[random.nextInt(6)] : "none";    // Description
        value[10] = LocalDateTime.now();                // Purchase_time
        return value;
    }

    public String Create_message(String User_ID, String Product_ID) {
        Object[] value = Gen_purchase_data(User_ID, Product_ID);
        
        String[] col_names = new String[] {"Purchase_ID", "User_ID", "Product_ID", "Shipping_cost",
                                            "Discount", "Total_cost", "Payment_method", "Quantity",
                                            "Payment_status", "Description", "Purchase_time"};

        
        for (int i = 0; i < this.arr.length; i++) {
            if (i == 1) this.arr[i] = value[i].toString();
            else this.arr[i] = "\"" + col_names[i] + "\":\"" + value[i].toString() + "\"";      
        }
        
        String message = "{" + String.join(",", this.arr) + "}";
        return message;
    }
}

