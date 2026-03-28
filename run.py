import argparse
import pandas as pd
import numpy as np
import yaml
import time
import logging
import json
import sys

def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def load_config(path):
    with open(path, 'r') as f:
        config = yaml.safe_load(f)

    if not all(k in config for k in ["seed", "window", "version"]):
        raise ValueError("Invalid config structure")

    return config

def load_data(path):
    df = pd.read_csv(path)

    if df.empty:
        raise ValueError("Empty dataset")

    if "close" not in df.columns:
        raise ValueError("Missing 'close' column")

    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)

    args = parser.parse_args()

    setup_logging(args.log_file)

    start_time = time.time()

    try:
        logging.info("Job started")

        config = load_config(args.config)
        np.random.seed(config["seed"])

        logging.info(f"Config loaded: {config}")

        df = load_data(args.input)
        logging.info(f"Rows loaded: {len(df)}")

        window = config["window"]

        df["rolling_mean"] = df["close"].rolling(window).mean()

        df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)

        df = df.dropna()

        signal_rate = df["signal"].mean()

        latency = int((time.time() - start_time) * 1000)

        result = {
            "version": config["version"],
            "rows_processed": int(len(df)),
            "metric": "signal_rate",
            "value": float(signal_rate),
            "latency_ms": latency,
            "seed": config["seed"],
            "status": "success"
        }

        logging.info(f"Metrics: {result}")

    except Exception as e:
        result = {
            "version": "v1",
            "status": "error",
            "error_message": str(e)
        }
        logging.error(str(e))

    with open(args.output, "w") as f:
        json.dump(result, f, indent=4)

    print(json.dumps(result, indent=4))

if __name__ == "__main__":
    main()