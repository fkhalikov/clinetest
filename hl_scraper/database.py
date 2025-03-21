import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")


def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


def create_table(cur, table_name):
    cur.execute(f"SELECT to_regclass('{table_name}')")
    table_exists = cur.fetchone()[0]

    if not table_exists:
        # Define the table schema based on the provided JSON structure
        cur.execute(f"""
            CREATE TABLE {table_name} (
                sedol VARCHAR(20),
                citicode VARCHAR(20),
                full_description TEXT,
                ismininv_monthly VARCHAR(20),
                reg_saver VARCHAR(20),
                closed_fund VARCHAR(20),
                fund_name TEXT,
                aims TEXT,
                description TEXT,
                company_id VARCHAR(20),
                company_name TEXT,
                sector_id VARCHAR(20),
                sector_name TEXT,
                unit_type VARCHAR(20),
                full_description_with_unit_type TEXT,
                initial_charge VARCHAR(20),
                annual_charge VARCHAR(20),
                vantage_charge VARCHAR(20),
                net_annual_charge VARCHAR(20),
                initial_saving VARCHAR(20),
                annual_saving VARCHAR(20),
                holding_fee VARCHAR(20),
                loaded VARCHAR(20),
                linksedol_code VARCHAR(20),
                restrict_ot VARCHAR(20),
                payment_frequency VARCHAR(20),
                payment_type VARCHAR(20),
                Wealth150 VARCHAR(20),
                trustee VARCHAR(20),
                assumed_growth VARCHAR(20),
                assumed_growth2 VARCHAR(20),
                valuation_point VARCHAR(20),
                comment TEXT,
                tracker VARCHAR(20),
                yieldreduct VARCHAR(20),
                yieldreduct2 VARCHAR(20),
                launch_price VARCHAR(20),
                launch_currency VARCHAR(20),
                num_holdings VARCHAR(20),
                icvc VARCHAR(20),
                sicav VARCHAR(20),
                plusfund VARCHAR(20),
                standard_amc VARCHAR(20),
                standard_ocf VARCHAR(20),
                total_expenses VARCHAR(20),
                charge_source VARCHAR(20),
                valuation_frequency VARCHAR(20),
                val_info TEXT,
                lsmininv VARCHAR(20),
                yield VARCHAR(20),
                kiid VARCHAR(20),
                kiid_url TEXT,
                running_yield VARCHAR(20),
                historic_yield VARCHAR(20),
                distribution_yield VARCHAR(20),
                underlying_yield VARCHAR(20),
                gross_yield VARCHAR(20),
                gross_running_yield VARCHAR(20),
                loyalty_bonus VARCHAR(20),
                reg_saver_min_inv VARCHAR(20),
                lump_sum_min_inv VARCHAR(20),
                renewal_commission VARCHAR(20),
                initial_commission VARCHAR(20),
                fund_size VARCHAR(20),
                risk_rating VARCHAR(20),
                other_expenses VARCHAR(20),
                cost_segment VARCHAR(20),
                update_time VARCHAR(20),
                ref_date VARCHAR(20),
                perf3m VARCHAR(20),
                perf6m VARCHAR(20),
                perf12m VARCHAR(20),
                perf36m VARCHAR(20),
                perf60m VARCHAR(20),
                perf120m VARCHAR(20),
                perf0t12m VARCHAR(20),
                perf12t24m VARCHAR(20),
                perf24t36m VARCHAR(20),
                perf36t48m VARCHAR(20),
                perf48t60m VARCHAR(20),
                ter VARCHAR(20),
                is_unit_trust VARCHAR(20),
                is_oeic VARCHAR(20),
                isaable VARCHAR(20),
                sippable VARCHAR(20),
                unwrapable VARCHAR(20),
                launchdate VARCHAR(20),
                sales_only VARCHAR(20),
                bid_price VARCHAR(20),
                offer_price VARCHAR(20),
                price_change VARCHAR(20),
                percent_change VARCHAR(20),
                updated VARCHAR(20),
                investment_pathway VARCHAR(20),
                    is_deleted BOOLEAN DEFAULT FALSE
                )
            """)
        print(f"Table {table_name} created successfully!")
    
    cur.execute(f"""
        ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE
    """)


def save_data(data):
    conn = connect_to_db()
    if conn is None:
        return

    try:
        cur = conn.cursor()

        # Define the table name
        table_name = "hl_funds"

        create_table(cur, table_name)

        # Prepare the insert query
        columns = ", ".join(data.keys())
        values = ", ".join(["%s"] * len(data))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

        # Check if the fund already exists based on SEDOL and updated date
        cur.execute(f"""
            SELECT sedol FROM {table_name}
            WHERE sedol = %s
            AND updated = %s
            AND is_deleted = FALSE
        """, (data['sedol'], data['updated']))
        existing_fund = cur.fetchone()

        if existing_fund:
            print(f"Fund with SEDOL {data['sedol']} and updated date {data['updated']} already exists. Skipping insertion.")
            return

        # Execute the insert query
        try:
            cur.execute(insert_query, list(data.values()))
            conn.commit()
            print("Data saved to database successfully!")
        except Exception as e:
            print(f"Error saving data to database: {e}")

    except Exception as e:
        print(f"Error checking or creating table: {e}")

    finally:
        if conn:
            cur.close()
            conn.close()
