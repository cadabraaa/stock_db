from sqlalchemy import create_engine, text
import os
import datetime

db_connection_string = os.environ['DB_CONNECTION_STRING']
engine = create_engine(db_connection_string)

def insert_data_into_database(data):
  
  # Create the transaction outside the loop
  with engine.connect() as conn:
    transaction = conn.begin()


    try:
      for record in data.get('filtered', {}).get('data', []):

        # Insert "PE" record
        query_pe = text(
            "INSERT INTO stock_table (Dateandtime, Strike_Price, Calltype, Expiry_Date, Underlying, "
            "Identifier, Open_Interest, Changes, Implied_Volatility, "
            "Last_Price, Total_Buy_Quantity, Total_Sell_Quantity, Bid_Qty, "
            "Bid_Price, Ask_Qty, Ask_Price, Underlying_Value) "
            "VALUES (:dateandtime, :strike_price, 'PE', :expiry_date, :underlying, "
            ":identifier, :open_interest, :changes, :implied_volatility, "
            ":last_price, :total_buy_quantity, :total_sell_quantity, :bid_qty, "
            ":bid_price, :ask_qty, :ask_price, :underlying_value)")

        params_pe = {
            'dateandtime':record.get('PE', {}).get('timestamp', ''),
            'strike_price':record.get('strikePrice'),
            'expiry_date':record.get('expiryDate'),
            'underlying':record.get('PE', {}).get('underlying', ''),
            'identifier':record.get('PE', {}).get('identifier', ''),
            'open_interest':record.get('PE', {}).get('openInterest', ''),
            'changes':record.get('PE', {}).get('changeinOpenInterest', ''),
            'implied_volatility':record.get('PE', {}).get('impliedVolatility', ''),
            'last_price':record.get('PE', {}).get('lastPrice', ''),
            'total_buy_quantity':record.get('PE', {}).get('totalBuyQuantity', ''),
            'total_sell_quantity':record.get('PE', {}).get('totalSellQuantity', ''),
            'bid_qty':record.get('PE', {}).get('bidQty', ''),
            'bid_price':record.get('PE', {}).get('bidprice', ''),
            'ask_qty':record.get('PE', {}).get('askQty', ''),
            'ask_price':record.get('PE', {}).get('askPrice', ''),
            'underlying_value':record.get('PE', {}).get('underlyingValue', '')
        }

        # Execute the "PE" query
        try:
          conn.execute(query_pe, params_pe)
          print("Insert PE successful!")
        except Exception as e:
          print(f"Error during PE insertion: {e}")

        # Insert "CE" record
        query_ce = text(
            "INSERT INTO stock_table (Dateandtime, Strike_Price, Calltype, Expiry_Date, Underlying, "
            "Identifier, Open_Interest, Changes, Implied_Volatility, "
            "Last_Price, Total_Buy_Quantity, Total_Sell_Quantity, Bid_Qty, "
            "Bid_Price, Ask_Qty, Ask_Price, Underlying_Value) "
            "VALUES (:dateandtime, :strike_price, 'CE', :expiry_date, :underlying, "
            ":identifier, :open_interest, :changes, :implied_volatility, "
            ":last_price, :total_buy_quantity, :total_sell_quantity, :bid_qty, "
            ":bid_price, :ask_qty, :ask_price, :underlying_value)")

        params_ce = {
            'dateandtime':record.get('CE', {}).get('timestamp', ''),
            'strike_price':record.get('strikePrice'),
            'expiry_date':record.get('expiryDate'),
            'underlying':record.get('CE', {}).get('underlying', ''),
            'identifier':record.get('CE', {}).get('identifier', ''),
            'open_interest':record.get('CE', {}).get('openInterest', ''),
            'changes':record.get('CE', {}).get('changeinOpenInterest', ''),
            'implied_volatility':record.get('CE', {}).get('impliedVolatility', ''),
            'last_price':record.get('CE', {}).get('lastPrice', ''),
            'total_buy_quantity':record.get('CE', {}).get('totalBuyQuantity', ''),
            'total_sell_quantity':record.get('CE', {}).get('totalSellQuantity', ''),
            'bid_qty':record.get('CE', {}).get('bidQty', ''),
            'bid_price':record.get('CE', {}).get('bidprice', ''),
            'ask_qty':record.get('CE', {}).get('askQty', ''),
            'ask_price':record.get('CE', {}).get('askPrice', ''),
            'underlying_value':record.get('CE', {}).get('underlyingValue', '')
        }

        # Execute the "CE" query
        try:
          conn.execute(query_ce, params_ce)
          print("Insert CE successful!")
        except Exception as e:
          print(f"Error during CE insertion: {e}")

      # Commit the transaction after the loop
      transaction.commit()

    except Exception as e:
      # Rollback if an error occurs outside the loop
      transaction.rollback()
      print(f"Error outside the loop: {e}")
