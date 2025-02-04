package Warehouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

public class Step4EnrichTransaction {

    private Connection connection;

    public Step4EnrichTransaction(String url, String user, String password) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            this.connection = DriverManager.getConnection(url, user, password);
            System.out.println("Database connection established.");
        } catch (Exception e) {
            System.out.println("Error establishing database connection.");
            e.printStackTrace();
        }
    }

    public void enrichTransactions(HashMap<Integer, String> transactionStream, HashMap<Integer, String> customerData, HashMap<Integer, String[]> productData) {
        System.out.println("Enriched Transactions with TOTAL_SALE:");
        for (Integer transactionId : transactionStream.keySet()) {
            try {
                String transactionData = transactionStream.get(transactionId);
                String[] attributes = transactionData.split(", ");
                int productId = Integer.parseInt(attributes[0].split(": ")[1]);
                int customerId = Integer.parseInt(attributes[1].split(": ")[1]);

                if (customerData.containsKey(customerId) && productData.containsKey(productId)) {
                    String[] productDetails = productData.get(productId);
                    double productPrice = Double.parseDouble(productDetails[1]);


                    String sql = "SELECT quantity_ordered FROM transactions_fact WHERE transaction_id = " + transactionId;
                    Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(sql);

                    if (rs.next()) {
                        int quantityOrdered = rs.getInt("quantity_ordered");

                        double totalSale = productPrice * quantityOrdered;

                        System.out.println("Transaction ID: " + transactionId + ", Product ID: " + productId +
                                ", Customer ID: " + customerId + ", Quantity Ordered: " + quantityOrdered +
                                ", Product Price: " + productPrice + ", TOTAL_SALE: " + totalSale);
                    }
                } else {
                    System.out.println("Transaction ID: " + transactionId + " has no matching customer or product.");
                }
            } catch (Exception e) {
                System.out.println("Error enriching transaction with ID: " + transactionId);
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {

        String url = "jdbc:mysql://localhost:3306/proj?allowLoadLocalInfile=true";
        String user = "root"; 
        String password = "hehe1234"; 

        Step2LoadMDPartitions mdLoader = new Step2LoadMDPartitions();
        HashMap<Integer, String> customerData = mdLoader.loadAllCustomerData(url, user, password);
        HashMap<Integer, String[]> productData = mdLoader.loadAllProductData(url, user, password);

        Step1TransactionStream transactionLoader = new Step1TransactionStream();
        HashMap<Integer, String> transactionStream = transactionLoader.readTransactionSegment(url, user, password, 0);

        Step4EnrichTransaction enricher = new Step4EnrichTransaction(url, user, password);
        enricher.enrichTransactions(transactionStream, customerData, productData);
    }
}
