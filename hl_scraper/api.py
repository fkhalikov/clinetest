from flask import Flask, jsonify
from flask_cors import CORS
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")

@app.route('/funds')
def get_funds():
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()
        
        # Get the ordering and filtering parameters from the request
        order_by = request.args.get('order_by', 'fund_name')
        order_dir = request.args.get('order_dir', 'asc')
        filter_term = request.args.get('filter', '')

        # Validate order_by and order_dir to prevent SQL injection
        valid_columns = ['sedol', 'fund_name', 'company_name', 'annual_charge', 'fund_size', 'perf12m', 'yield', 'bid_price', 'offer_price', 'updated']
        if order_by not in valid_columns:
            order_by = 'fund_name'  # Default to fund_name if invalid

        valid_directions = ['asc', 'desc']
        if order_dir not in valid_directions:
            order_dir = 'asc'  # Default to ascending if invalid
        
        # Construct the SQL query with ordering and filtering
        query = "SELECT * FROM hl_funds"
        if filter_term:
            query += f" WHERE fund_name ILIKE '%{filter_term}%' OR company_name ILIKE '%{filter_term}%' OR sedol ILIKE '%{filter_term}%'"
        query += f" ORDER BY {order_by} {order_dir}"

        cur.execute(query)
        funds = cur.fetchall()

        # Convert the data to a list of dictionaries
        column_names = [desc[0] for desc in cur.description]
        funds_list = [dict(zip(column_names, row)) for row in funds]

        return jsonify(funds_list)
    except Exception as e:
        return jsonify({'error': str(e)})
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    app.run(debug=True)

from flask import request
