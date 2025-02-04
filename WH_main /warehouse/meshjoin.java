package Warehouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

public class meshjoin {

    private static final int SEGMENT_SIZE = 10; 
    private Connection connection;

    public meshjoin(String url, String user, String password) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            this.connection = DriverManager.getConnection(url, user, password);
            System.out.println("Database connection established.");
        } catch (Exception e) {
            System.out.println("Error establishing database connection.");
            e.printStackTrace();
        }
    }

    
    private int getTotalRows() {
        int totalRows = 0;
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS total_rows FROM transactions_fact");
            if (rs.next()) {
                totalRows = rs.getInt("total_rows");
            }
        } catch (Exception e) {
            System.out.println("Error fetching total row count.");
            e.printStackTrace();
        }
        return totalRows;
    }

   
    public static void main(String[] args) {
       
        String url = "jdbc:mysql://localhost:3306/proj?allowLoadLocalInfile=true";
        String user = "root"; 
        String password = "hehe1234"; 

        meshjoin meshJoin = new meshjoin(url, user, password);

        
        int totalRows = meshJoin.getTotalRows();
        System.out.println("Total rows in transactions_fact: " + totalRows);

        
        Step2LoadMDPartitions mdLoader = new Step2LoadMDPartitions();
        HashMap<Integer, String> customerData = mdLoader.loadAllCustomerData(url, user, password);
        HashMap<Integer, String[]> productData = mdLoader.loadAllProductData(url, user, password);

        int offset = 0;
        boolean hasMoreData = true;

        while (hasMoreData) {
            
            Step1TransactionStream transactionLoader = new Step1TransactionStream();
            HashMap<Integer, String> transactionStream = transactionLoader.readTransactionSegment(url, user, password, offset);

           
            if (transactionStream.isEmpty() || offset >= totalRows) {
                hasMoreData = false; // No more data to process
                break;
            }

           
            Step4EnrichTransaction enricher = new Step4EnrichTransaction(url, user, password);
            enricher.enrichTransactions(transactionStream, customerData, productData);

            
            Step5LoadData loader = new Step5LoadData(url, user, password);
            loader.loadToDW(transactionStream, customerData, productData);

            
            offset += SEGMENT_SIZE;
        }

        System.out.println("All transactions processed successfully.");
    }
}
