package Warehouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

public class Step1TransactionStream {

    
    public static void main(String[] args) {
        
        String url = "jdbc:mysql://localhost:3306/proj?allowLoadLocalInfile=true";
        String user = "root"; 
        String password = "hehe1234"; 

        int offset = 0; 

        Step1TransactionStream transactionStreamReader = new Step1TransactionStream();
        HashMap<Integer, String> transactionSegment = transactionStreamReader.readTransactionSegment(url, user, password, offset);

        System.out.println("Transaction Segment Data:");
        transactionSegment.forEach((key, value) -> System.out.println("Transaction ID: " + key + " -> " + value));
    }

    public HashMap<Integer, String> readTransactionSegment(String url, String user, String password, int offset) {
        HashMap<Integer, String> transactionSegment = new HashMap<>();

        try {
            
            Class.forName("com.mysql.cj.jdbc.Driver");

            Connection connection = DriverManager.getConnection(url, user, password);

            String sql = "SELECT transaction_id, product_id, customer_id FROM transactions_fact " +
                         "LIMIT " + SEGMENT_SIZE + " OFFSET " + offset;

            System.out.println("Executing SQL: " + sql);

            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                int transactionId = rs.getInt("transaction_id");
                int productId = rs.getInt("product_id");
                int customerId = rs.getInt("customer_id");

                transactionSegment.put(transactionId, "Product ID: " + productId + ", Customer ID: " + customerId);
            }

            connection.close();
        } catch (Exception e) {
            System.out.println("Error while reading transaction segment");
            e.printStackTrace();
        }

        return transactionSegment;
    }

    private static final int SEGMENT_SIZE = 10; 
}
