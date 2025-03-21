import requests
import pandas as pd
import time
import random
import logging
from database import save_data

# Set up logging: output to file and console
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scraping.log", mode="w"),
        logging.StreamHandler()
    ]
)

# New AJAX endpoint for fund search
base_url = "https://www.hl.co.uk/ajax/funds/fund-search/search"

# Query parameters for the request
params = {
    "investment": "",
    "companyid": "",
    "sectorid": "",
    "wealth": "",
    "unitTypePref": "",
    "tracker": "",
    "payment_frequency": "",
    "payment_type": "",
    "yield": "",
    "standard_ocf": "",
    "perf12m": "",
    "perf36m": "",
    "perf60m": "",
    "fund_size": "",
    "num_holdings": "",
    "start": 0,
    "rpp": 20,
    "lo": 0,
    "sort": "fd.full_description",
    "sort_dir": "asc"
}

# List of sample User-Agent strings to rotate
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:102.0) Gecko/20100101 Firefox/102.0",
]

funds_data = []
page = 0
total_results = None

while total_results is None or page * params["rpp"] < total_results:
    # Randomize the User-Agent header for each request
    headers = {"User-Agent": random.choice(user_agents)}
    params["start"] = page * params["rpp"]
    
    logging.info(f"Requesting page {page+1} - URL: {base_url} with params: {params}")
    try:
        response = requests.get(base_url, headers=headers, params=params, timeout=10)
    except Exception as e:
        logging.exception("Request failed:")
        break

    logging.info(f"Status Code: {response.status_code}")
    
    if response.status_code != 200:
        logging.error(f"Error: Received status code {response.status_code}")
        break

    snippet = response.text[:200]
    logging.debug("Response snippet: %s", snippet)
    
    try:
        json_data = response.json()
    except Exception as e:
        logging.exception("Failed to decode JSON. Full response:")
        logging.debug(response.text)
        break

    if total_results is None:
        total_results = json_data.get("TotalResults", 0)
        logging.info(f"Total results: {total_results}")
    
    for fund in json_data.get("Results", []):
        fund_data = {
            "sedol": fund.get("sedol", ""),
            "citicode": fund.get("citicode", ""),
            "full_description": fund.get("full_description", ""),
            "ismininv_monthly": fund.get("ismininv_monthly", ""),
            "reg_saver": fund.get("reg_saver", ""),
            "closed_fund": fund.get("closed_fund", ""),
            "fund_name": fund.get("fund_name", ""),
            "aims": fund.get("aims", ""),
            "description": fund.get("description", ""),
            "company_id": fund.get("company_id", ""),
            "company_name": fund.get("company_name", ""),
            "sector_id": fund.get("sector_id", ""),
            "sector_name": fund.get("sector_name", ""),
            "unit_type": fund.get("unit_type", ""),
            "full_description_with_unit_type": fund.get("full_description_with_unit_type", ""),
            "initial_charge": fund.get("initial_charge", ""),
            "annual_charge": fund.get("annual_charge", ""),
            "vantage_charge": fund.get("vantage_charge", ""),
            "net_annual_charge": fund.get("net_annual_charge", ""),
            "initial_saving": fund.get("initial_saving", ""),
            "annual_saving": fund.get("annual_saving", ""),
            "holding_fee": fund.get("holding_fee", ""),
            "loaded": fund.get("loaded", ""),
            "linksedol_code": fund.get("linksedol_code", ""),
            "restrict_ot": fund.get("restrict_ot", ""),
            "payment_frequency": fund.get("payment_frequency", ""),
            "payment_type": fund.get("payment_type", ""),
            "Wealth150": fund.get("Wealth150", ""),
            "trustee": fund.get("trustee", ""),
            "assumed_growth": fund.get("assumed_growth", ""),
            "assumed_growth2": fund.get("assumed_growth2", ""),
            "valuation_point": fund.get("valuation_point", ""),
            "comment": fund.get("comment", ""),
            "tracker": fund.get("tracker", ""),
            "yieldreduct": fund.get("yieldreduct", ""),
            "yieldreduct2": fund.get("yieldreduct2", ""),
            "launch_price": fund.get("launch_price", ""),
            "launch_currency": fund.get("launch_currency", ""),
            "num_holdings": fund.get("num_holdings", ""),
            "icvc": fund.get("icvc", ""),
            "sicav": fund.get("sicav", ""),
            "plusfund": fund.get("plusfund", ""),
            "standard_amc": fund.get("standard_amc", ""),
            "standard_ocf": fund.get("standard_ocf", ""),
            "total_expenses": fund.get("total_expenses", ""),
            "charge_source": fund.get("charge_source", ""),
            "valuation_frequency": fund.get("valuation_frequency", ""),
            "val_info": fund.get("val_info", ""),
            "lsmininv": fund.get("lsmininv", ""),
            "yield": fund.get("yield", ""),
            "kiid": fund.get("kiid", ""),
            "kiid_url": fund.get("kiid_url", ""),
            "running_yield": fund.get("running_yield", ""),
            "historic_yield": fund.get("historic_yield", ""),
            "distribution_yield": fund.get("distribution_yield", ""),
            "underlying_yield": fund.get("underlying_yield", ""),
            "gross_yield": fund.get("gross_yield", ""),
            "gross_running_yield": fund.get("gross_running_yield", ""),
            "loyalty_bonus": fund.get("loyalty_bonus", ""),
            "reg_saver_min_inv": fund.get("reg_saver_min_inv", ""),
            "lump_sum_min_inv": fund.get("lump_sum_min_inv", ""),
            "renewal_commission": fund.get("renewal_commission", ""),
            "initial_commission": fund.get("initial_commission", ""),
            "fund_size": fund.get("fund_size", ""),
            "risk_rating": fund.get("risk_rating", ""),
            "other_expenses": fund.get("other_expenses", ""),
            "cost_segment": fund.get("cost_segment", ""),
            "update_time": fund.get("update_time", ""),
            "ref_date": fund.get("ref_date", ""),
            "perf3m": fund.get("perf3m", ""),
            "perf6m": fund.get("perf6m", ""),
            "perf12m": fund.get("perf12m", ""),
            "perf36m": fund.get("perf36m", ""),
            "perf60m": fund.get("perf60m", ""),
            "perf120m": fund.get("perf120m", ""),
            "perf0t12m": fund.get("perf0t12m", ""),
            "perf12t24m": fund.get("perf12t24m", ""),
            "perf24t36m": fund.get("perf24t36m", ""),
            "perf36t48m": fund.get("perf36t48m", ""),
            "perf48t60m": fund.get("perf48t60m", ""),
            "perf3m_reinv": fund.get("perf3m_reinv", ""),
            "perf6m_reinv": fund.get("perf6m_reinv", ""),
            "perf12m_reinv": fund.get("perf12m_reinv", ""),
            "perf36m_reinv": fund.get("perf36m_reinv", ""),
            "perf60m_reinv": fund.get("perf60m_reinv", ""),
            "perf120m_reinv": fund.get("perf120m_reinv", ""),
            "perf0t12m_reinv": fund.get("perf0t12m_reinv", ""),
            "perf12t24m_reinv": fund.get("perf12t24m_reinv", ""),
            "perf24t36m_reinv": fund.get("perf24t36m_reinv", ""),
            "perf36t48m_reinv": fund.get("perf36t48m_reinv", ""),
            "perf48t60m_reinv": fund.get("perf48t60m_reinv", ""),
            "ter": fund.get("ter", ""),
            "is_unit_trust": fund.get("is_unit_trust", ""),
            "is_oeic": fund.get("is_oeic", ""),
            "isaable": fund.get("isaable", ""),
            "sippable": fund.get("sippable", ""),
            "unwrapable": fund.get("unwrapable", ""),
            "launchdate": fund.get("launchdate", ""),
            "sales_only": fund.get("sales_only", ""),
            "bid_price": fund.get("bid_price", ""),
            "offer_price": fund.get("offer_price", ""),
            "price_change": fund.get("price_change", ""),
            "percent_change": fund.get("percent_change", ""),
            "updated": fund.get("updated", ""),
            "investment_pathway": fund.get("investment_pathway", "")
        }
        funds_data.append(fund_data)
        try:
            save_data(fund_data)
        except Exception as e:
            logging.error(f"Error saving data for fund {fund_data.get('sedol', 'Unknown')}: {e}")

    logging.info(f"Scraped {len(funds_data)} funds so far...")
    page += 1
    # Sleep for a random time between 1 and 3 seconds to reduce request frequency
    delay = random.uniform(1, 3)
    logging.debug(f"Sleeping for {delay:.2f} seconds")
    time.sleep(delay)

if funds_data:
    logging.info("Scraping complete!")
else:
    logging.warning("No fund data was scraped.")
