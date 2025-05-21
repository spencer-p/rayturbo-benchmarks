"""Tested on 2.46"""
import ray
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.aggregate import Mean, Min, Max, Unique

import argparse
import time

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TPCH Q1")
    parser.add_argument("--sf", choices=[100, 1000, 10000], type=int, default=100)
    parser.add_argument("--enable_hash_shuffle", action="store_true")
    return parser.parse_args()

def main(args):
    dataset_path = f"s3://ray-benchmark-data/tpch/parquet/sf{args.sf}/orders"
    if args.enable_hash_shuffle:
        DataContext.get_current().shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    print("Start of execution")
    start = time.time()

    ds = ray.data.read_parquet(dataset_path)
    ds = ds.rename_columns({
        "column0": "ORDERKEY",
        "column1": "CUSTKEY",
        "column2": "ORDERSTATUS",
        "column3": "TOTALPRICE",
        "column4": "ORDERDATE",
        "column5": "ORDER-PRIORITY",
        "column6": "CLERK",
        "column7": "SHIP-PRIORITY",
        "column8": "COMMENT"
    })
    print(ds.groupby("CUSTKEY").aggregate(
        Max("TOTALPRICE"), Min("TOTALPRICE"), Mean("TOTALPRICE"), Unique("ORDER-PRIORITY")))

    print("End of execution", time.time() - start)

if __name__ == "__main__":
    args = parse_args()
    main(args)
