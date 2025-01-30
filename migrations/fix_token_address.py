import sqlite3
import os
import sys
import logging

logging.basicConfig(level=logging.INFO)

def migrate_database():
    DB_FILE = "solana_transactions.db"
    BACKUP_FILE = "solana_transactions.db.backup"
    
    # Create backup
    if os.path.exists(DB_FILE):
        logging.info("Creating database backup...")
        with open(DB_FILE, 'rb') as src, open(BACKUP_FILE, 'wb') as dst:
            dst.write(src.read())
    
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Check if we need to rename the column
        c.execute("PRAGMA table_info(tokens)")
        columns = c.fetchall()
        column_names = [col[1] for col in columns]
        
        if 'address' in column_names and 'token_address' not in column_names:
            logging.info("Updating queries to use 'address' instead of 'token_address'...")
            
            # Update the analyze_deployer_history query
            with open('token_monitor_prime2.py', 'r') as file:
                content = file.read()
            
            # Replace the query
            old_query = """SELECT token_address, max_market_cap 
            FROM tokens 
            WHERE deployer_address = ?"""
            
            new_query = """SELECT address, peak_market_cap 
            FROM tokens 
            WHERE deployer_address = ?"""
            
            content = content.replace(old_query, new_query)
            
            with open('token_monitor_prime2.py', 'w') as file:
                file.write(content)
            
            logging.info("Successfully updated queries!")
            
        conn.commit()
        logging.info("Migration completed successfully!")
        
    except Exception as e:
        logging.error(f"Error during migration: {e}")
        # Restore from backup if something went wrong
        if os.path.exists(BACKUP_FILE):
            logging.info("Restoring from backup...")
            with open(BACKUP_FILE, 'rb') as src, open(DB_FILE, 'wb') as dst:
                dst.write(src.read())
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_database()
