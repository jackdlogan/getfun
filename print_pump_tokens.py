import sqlite3
from datetime import datetime
import csv

def print_pump_tokens():
    # Connect to the SQLite database
    conn = sqlite3.connect('pump_portal.db')
    cursor = conn.cursor()

    try:
        # First, let's check what tables exist in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables_in_db = cursor.fetchall()
        print("\nTables found in database:", [table[0] for table in tables_in_db])

        # Get all records from each table
        tables = {
            'new_tokens': '''SELECT * FROM new_tokens ORDER BY timestamp DESC''',
            'account_trades': '''SELECT * FROM account_trades ORDER BY timestamp DESC''',
            'token_trades': '''SELECT * FROM token_trades ORDER BY timestamp DESC'''
        }
        
        for table_name, query in tables.items():
            print(f"\nQuerying {table_name}...")
            try:
                cursor.execute(query)
                # Get column names
                columns = [description[0] for description in cursor.description]
                print(f"Columns in {table_name}: {columns}")
                
                records = cursor.fetchall()
                
                if not records:
                    print(f"No records found in {table_name}")
                    continue

                # Print to console
                print(f"\n=== {table_name.upper()} Records ===")
                
                # Save to CSV
                csv_filename = f'pump_portal_{table_name}.csv'
                with open(csv_filename, 'w', newline='') as csvfile:
                    csv_writer = csv.writer(csvfile)
                    
                    # Write headers (using actual column names)
                    csv_writer.writerow(columns)
                    print("HEADERS:", "\t".join(columns))
                    print("-" * 120)
                    
                    # Write and print all records
                    for record in records:
                        csv_writer.writerow(record)
                        print("\t".join(str(field) for field in record))
                
                print(f"\nData saved to {csv_filename}")
                print(f"Total records in {table_name}: {len(records)}")

            except sqlite3.Error as table_error:
                print(f"Error querying {table_name}: {table_error}")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    
    finally:
        conn.close()

if __name__ == "__main__":
    print_pump_tokens() 