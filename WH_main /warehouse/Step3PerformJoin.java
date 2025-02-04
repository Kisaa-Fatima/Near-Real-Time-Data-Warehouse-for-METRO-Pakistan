package Warehouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

public class Step3PerformJoin {

    private static final int PARTITION_SIZE = 10; 
    private Connection connection;

    public Step3PerformJoin(String url, String user, String password) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            this.connection = DriverManager.getConnection(url, user, password);
            System.out.println("Database connection established.");
        } catch (Exception e) {
            System.out.println("Error establishing database connection.");
            e.printStackTrace();
        }
    }

    public HashMap<Integer, String> loadCustomerData() {
        HashMap<Integer, String> customerData = new HashMap<>();
        try {
            String sql = "SELECT customer_id, TRIM(customer_name) AS customer_name, TRIM(gender) AS gender FROM customer_dim";
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                int customerId = rs.getInt("customer_id");
                String data = "Name: " + rs.getString("customer_name") + ", Gender: " + rs.getString("gender");
                customerData.put(customerId, data);
            }

            System.out.println("Loaded Customer Data: " + customerData);
        } catch (Exception e) {
            System.out.println("Error loading customer data.");
            e.printStackTrace();
        }
        return customerData;
    }

    public HashMap<Integer, String> loadProductData() {
        HashMap<Integer, String> productData = new HashMap<>();
        try {
            String sql = "SELECT product_id, TRIM(product_name) AS product_name, product_price FROM product_data";
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                int productId = rs.getInt("product_id");
                String data = "Name: " + rs.getString("product_name") + ", Price: " + rs.getDouble("product_price");
                productData.put(productId, data);
            }

            System.out.println("Loaded Product Data: " + productData);
        } catch (Exception e) {
            System.out.println("Error loading product data.");
            e.printStackTrace();
        }
        return productData;
    }

    public HashMap<Integer, String> loadTransactionStream(int offset) {
        HashMap<Integer, String> transactionStream = new HashMap<>();
        try {
            String sql = "SELECT transaction_id, product_id, customer_id FROM transactions_fact " +
                         "LIMIT " + PARTITION_SIZE + " OFFSET " + offset;
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                int transactionId = rs.getInt("transaction_id");
                int productId = rs.getInt("product_id");
                int customerId = rs.getInt("customer_id");
                transactionStream.put(transactionId, "Product ID: " + productId + ", Customer ID: " + customerId);
            }

            System.out.println("Loaded Transaction Stream: " + transactionStream);
        } catch (Exception e) {
            System.out.println("Error loading transaction stream.");
            e.printStackTrace();
        }
        return transactionStream;
    }

    public void performJoin(int transactionOffset) {
        
        HashMap<Integer, String> customerData = loadCustomerData();
        HashMap<Integer, String> productData = loadProductData();

        HashMap<Integer, String> transactionStream = loadTransactionStream(transactionOffset);

        // Enrich transactions
        System.out.println("Enriched Transactions:");
        for (Integer transactionId : transactionStream.keySet()) {
            String transactionData = transactionStream.get(transactionId);
            String[] attributes = transactionData.split(", ");
            int productId = Integer.parseInt(attributes[0].split(": ")[1]);
            int customerId = Integer.parseInt(attributes[1].split(": ")[1]);

            if (customerData.containsKey(customerId) && productData.containsKey(productId)) {
                String enrichedData = "Transaction ID: " + transactionId + ", " + transactionData +
                        ", Customer Data: {" + customerData.get(customerId) + "}, Product Data: {" + productData.get(productId) + "}";
                System.out.println(enrichedData);
            } else {
                System.out.println("Transaction ID: " + transactionId + " has no matching customer or product.");
            }
        }
    }

    public static void main(String[] args) {

        String url = "jdbc:mysql://localhost:3306/proj?allowLoadLocalInfile=true";
        String user = "root"; 
        String password = "hehe1234"; 

        Step3PerformJoin joiner = new Step3PerformJoin(url, user, password);

        joiner.performJoin(0); // Offset 0
    }
}
