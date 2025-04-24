import csv
import random
import datetime

def generate_uncleaned_data(num_rows=2000, output_file='transactions_uncleaned.csv'):
    """
    Generates an uncleaned dataset (with duplicates, missing values, invalid data, etc.)
    for ETL practice. Adds additional columns for business analysis.
    """
    # Predefined "dirty" values
    possible_product_ids = [None, 1, 2, 7, 8, 10, 10, 9999999, 5, 3, 2]
    possible_customer_ids = [None, 1, 2, 8, 8, 32, 133, -5, 999, 2, 10]

    # Business-related fields
    possible_sales_channels = [None, "online", "store", "reseller", "invalid_channel"]
    possible_discount_codes = [None, "WELCOME10", "STUDENT20", "HALFOFF", "", "NOTVALID"]
    possible_regions = [None, "North", "South", "East", "West", "Central", "invalid_region"]
    possible_payment_methods = [None, "credit_card", "debit_card", "paypal", "cash", "invalid_method"]
    possible_transaction_statuses = [None, "completed", "pending", "failed", "unknown"]

    # Product prices
    product_price_map = {
        1: 24.3, 2: 63.6, 3: 39.2, 5: 87.94, 7: 36.9,
        8: 50, 10: 99.99, 9999999: 9999999
    }

    rows = []

    for i in range(num_rows):
        # Duplicate or missing transaction_id logic
        if i % 5 == 0:
            transaction_id = 1 if i == 0 else random.choice([None, random.randint(1, i)])
        else:
            transaction_id = i + 1

        # Generate possibly dirty timestamp
        if random.random() < 0.15:
            timestamp_val = random.choice([None, "not_a_timestamp"])
        else:
            dt = datetime.datetime(
                2025,
                random.randint(1, 12),
                random.randint(1, 28),
                random.randint(0, 23),
                random.randint(0, 59),
                random.randint(0, 59)
            )
            if random.random() < 0.3:
                dt = dt.replace(microsecond=random.randint(0, 999999))
            timestamp_val = dt.isoformat(sep=' ')

        # Random product_id and quantity
        product_id = random.choice(possible_product_ids)
        quantity = random.choice([None, random.randint(-2, 10)])

        # Calculate price safely
        price = None
        if product_id is not None and quantity is not None:
            if product_id in product_price_map and isinstance(quantity, (int, float)):
                price = round(product_price_map[product_id] * quantity, 2)

        # Other random fields
        customer_id = random.choice(possible_customer_ids)
        sales_channel = random.choice(possible_sales_channels)
        discount_code = random.choice(possible_discount_codes)
        region = random.choice(possible_regions)
        payment_method = random.choice(possible_payment_methods)
        transaction_status = random.choice(possible_transaction_statuses)

        # Compose the row
        row = {
            "transaction_id": transaction_id,
            "timestamp": timestamp_val,
            "product_id": product_id,
            "quantity": quantity,
            "price": price,
            "customer_id": customer_id,
            "sales_channel": sales_channel,
            "discount_code": discount_code,
            "region": region,
            "payment_method": payment_method,
            "transaction_status": transaction_status
        }
        rows.append(row)

    # Write to CSV
    fieldnames = [
        "transaction_id", "timestamp", "product_id", "quantity",
        "price", "customer_id", "sales_channel", "discount_code",
        "region", "payment_method", "transaction_status"
    ]
    with open(output_file, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"Generated {num_rows} rows of uncleaned data in '{output_file}'.")

if __name__ == "__main__":
    generate_uncleaned_data(num_rows=20000, output_file='transactions_uncleaned.csv')
