from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "sneha_reddy",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

# Task 1: Generate Data 
def task_generate_data(**context):
    import random, uuid, os, pandas as pd
    from datetime import datetime, timedelta

    print(" Generating synthetic e-commerce data...")
    random.seed()
    OUTPUT = "/opt/airflow/scripts"
    os.makedirs(OUTPUT, exist_ok=True)

    cities    = ["Mumbai","Delhi","Bangalore","Pune","Hyderabad","Chennai"]
    statuses  = ["delivered","shipped","cancelled","returned"]
    gateways  = ["Razorpay","PayU","Paytm","UPI"]
    categories= ["Electronics","Fashion","Grocery","Books","Sports"]

    orders = []
    for i in range(1000):
        orders.append({
            "order_id":       str(uuid.uuid4()),
            "user_id":        str(uuid.uuid4()),
            "order_date":     (datetime(2024,1,1) + timedelta(days=random.randint(0,364))).strftime("%Y-%m-%d"),
            "status":         random.choices(statuses, weights=[55,20,15,10])[0],
            "shipping_city":  random.choice(cities),
            "category":       random.choice(categories),
            "final_amount":   round(random.uniform(200, 8000), 2),
            "item_count":     random.randint(1, 5),
            "payment_gateway":random.choice(gateways),
        })

    df = pd.DataFrame(orders)
    df.to_csv(f"{OUTPUT}/orders_raw.csv", index=False)

    print(f" Generated {len(df):,} orders")
    print(f"   Saved to: {OUTPUT}/orders_raw.csv")

    # Push stats to XCom for downstream tasks
    context["ti"].xcom_push(key="row_count", value=len(df))
    return len(df)


# Task 2: Run ETL
def task_run_etl(**context):
    import pandas as pd

    print("  Running ETL transformations...")
    df = pd.read_csv("/opt/airflow/scripts/orders_raw.csv")

    # Transformations
    df["status"]       = df["status"].str.upper()
    df["final_amount"] = df["final_amount"].astype(float)
    df["order_date"]   = pd.to_datetime(df["order_date"])
    df["order_year"]   = df["order_date"].dt.year
    df["order_month"]  = df["order_date"].dt.month
    df["order_day"]    = df["order_date"].dt.day
    df["is_delivered"] = df["status"] == "DELIVERED"
    df["is_cancelled"] = df["status"] == "CANCELLED"
    df["is_returned"]  = df["status"] == "RETURNED"

    # Remove duplicates and nulls
    before = len(df)
    df = df.dropna(subset=["order_id","user_id"])
    df = df.drop_duplicates(subset=["order_id"])
    df = df[df["final_amount"] > 0]
    after = len(df)

    df.to_csv("/opt/airflow/scripts/orders_clean.csv", index=False)

    print(f" ETL complete")
    print(f"   Before: {before:,} | After: {after:,} | Removed: {before-after:,}")
    return after


# Task 3: Data Quality Check
def task_quality_check(**context):
    import pandas as pd

    print(" Running data quality checks...")
    df = pd.read_csv("/opt/airflow/scripts/orders_clean.csv")

    checks = {
        "No null order_ids":     df["order_id"].isna().sum() == 0,
        "No negative revenue":   (df["final_amount"] < 0).sum() == 0,
        "Row count > 100":       len(df) > 100,
        "Valid statuses only":   df["status"].isin(
                                     ["DELIVERED","SHIPPED","CANCELLED","RETURNED"]
                                 ).all(),
    }

    all_passed = True
    for check, passed in checks.items():
        icon = "Passed" if passed else "Failed"
        print(f"   {icon} {check}")
        if not passed:
            all_passed = False

    if not all_passed:
        raise ValueError("Data quality checks FAILED — pipeline stopped!")

    print(f"\n All checks passed — {len(df):,} clean rows")


# Task 4: Compute Aggregates
def task_compute_aggregates(**context):
    import pandas as pd

    print(" Computing business aggregates...")
    df = pd.read_csv("/opt/airflow/scripts/orders_clean.csv")
    delivered = df[df["status"] == "DELIVERED"]

    # Daily revenue
    daily = (
        delivered
        .groupby(["order_year","order_month","order_day","shipping_city","category"])
        .agg(
            orders        =("order_id",     "count"),
            revenue       =("final_amount", "sum"),
            avg_order_val =("final_amount", "mean"),
            customers     =("user_id",      "nunique"),
        )
        .reset_index()
        .round(2)
    )
    daily.to_csv("/opt/airflow/scripts/daily_revenue.csv", index=False)

    # Customer segments (simple RFM)
    customer_stats = (
        delivered
        .groupby("user_id")
        .agg(
            order_count  =("order_id",     "count"),
            total_spend  =("final_amount", "sum"),
            avg_spend    =("final_amount", "mean"),
        )
        .reset_index()
    )
    customer_stats["segment"] = pd.cut(
        customer_stats["total_spend"],
        bins=[0, 1000, 5000, 15000, float("inf")],
        labels=["Bronze","Silver","Gold","Platinum"]
    )
    customer_stats.to_csv("/opt/airflow/scripts/customer_segments.csv", index=False)

    print(f" Aggregates computed")
    print(f"\n--- Revenue by City ---")
    city_rev = delivered.groupby("shipping_city")["final_amount"].sum().sort_values(ascending=False)
    for city, rev in city_rev.items():
        print(f"   {city:<15} ₹{rev:>12,.0f}")

    print(f"\n--- Customer Segments ---")
    print(customer_stats["segment"].value_counts().to_string())


# Task 5: Generate Report
def task_generate_report(**context):
    import pandas as pd
    from datetime import datetime

    print("Generating pipeline summary report...")

    orders    = pd.read_csv("/opt/airflow/scripts/orders_clean.csv")
    revenue   = pd.read_csv("/opt/airflow/scripts/daily_revenue.csv")
    customers = pd.read_csv("/opt/airflow/scripts/customer_segments.csv")

    delivered = orders[orders["status"] == "DELIVERED"]

    report = f"""
╔══════════════════════════════════════════════════╗
║       SMARTCOMMERCE PIPELINE REPORT              ║
║       Run: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}              ║
╚══════════════════════════════════════════════════╝

PIPELINE SUMMARY
  Total orders processed : {len(orders):,}
  Delivered orders       : {len(delivered):,}
  Cancellation rate      : {orders['is_cancelled'].mean()*100:.1f}%
  Return rate            : {orders['is_returned'].mean()*100:.1f}%

REVENUE
  Total revenue          : ₹{delivered['final_amount'].sum():>12,.0f}
  Average order value    : ₹{delivered['final_amount'].mean():>12,.0f}
  Top city               : {delivered.groupby('shipping_city')['final_amount'].sum().idxmax()}
  Top category           : {delivered.groupby('category')['final_amount'].sum().idxmax()}

DATA QUALITY
  Clean rows             : {len(orders):,}
  Null values            : {orders.isna().sum().sum()}
  Duplicate orders       : 0

STATUS: PIPELINE COMPLETED SUCCESSFULLY
"""
    print(report)

    with open("/opt/airflow/scripts/pipeline_report.txt", "w") as f:
        f.write(report)

    print("Report saved to /opt/airflow/scripts/pipeline_report.txt")


# DAG Definition 
with DAG(
    dag_id="smartcommerce_pipeline",
    default_args=default_args,
    description="SmartCommerce end-to-end data pipeline",
    schedule_interval="* * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["smartcommerce", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")

    generate = PythonOperator(
        task_id="generate_data",
        python_callable=task_generate_data,
    )

    etl = PythonOperator(
        task_id="run_etl",
        python_callable=task_run_etl,
    )

    quality = PythonOperator(
        task_id="quality_check",
        python_callable=task_quality_check,
    )

    aggregates = PythonOperator(
        task_id="compute_aggregates",
        python_callable=task_compute_aggregates,
    )

    report = PythonOperator(
        task_id="generate_report",
        python_callable=task_generate_report,
    )

    end = EmptyOperator(task_id="end")

    # Pipeline flow
    start >> generate >> etl >> quality >> aggregates >> report >> end