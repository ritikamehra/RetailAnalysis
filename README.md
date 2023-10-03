# RetailAnalysis
## Goal:
Computing various Key Performance Indicators (KPIs) for an e-commerce company, RetailCorp Inc, based on real-time sales data of the company across the globe. The data contains information related to the invoices of orders placed by customers all around the world. 
 
![image](https://github.com/ritikamehra/RetailAnalysis/assets/54076372/abe0f6eb-82bf-40e2-90b4-b5eb44932351)

## Data Dictionary:
The data is based on an Online Retail Data Set in the UCI Machine Learning Repository. Each order’s invoice has been represented in a JSON format. The sample data:
{

  "items": [
  
  {
  
      "SKU": "21485",
      
      "title": "RETROSPOT HEART HOT WATER BOTTLE",
      
      "unit_price": 4.95,
      
      "quantity": 6
      
    },
    
    {
    
      "SKU": "23499",
      
      "title": "SET 12 VINTAGE DOILY CHALK",
      
      "unit_price": 0.42,
      
      "quantity": 2
      
    }
    
  ],
  
  "type": "ORDER"
  
  "country": "United Kingdom",
  
  "invoice_no": 154132541653705,
  
  "timestamp": "2020-09-18 10:55:23",
  
}

The data contains the following information:
Invoice number: Identifier of the invoice
Country: Country where the order is placed
Timestamp: Time at which the order is placed
Type: Whether this is a new order or a return order
SKU (Stock Keeping Unit): Identifier of the product being ordered
Title: Name of the product is ordered
Unit price: Price of a single unit of the product
Quantity: Quantity of the product being ordered

Broadly, the following tasks are performed in this project:
1. Reading the sales data from the Kafka server.
2. Preprocessing the data to calculate additional derived columns: total_cost, total_items, is_order, is_return.
3. Calculating the time-based KPIs and time and country-based KPIs such as Orders per minute, total_sale_volume, average_transaction_size etc.
4. Storing the KPIs (both time-based and time-and country-based) for a 10-minute interval into separate JSON files for further analysis.
