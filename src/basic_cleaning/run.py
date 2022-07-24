#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="baisc_cleaning")
    run.config.update(args)

    logger.info("Fetching artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("Reading data as dataframe")
    df = pd.read_csv(artifact_local_path)

    logger.info("Drop outliers")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    logger.info("Covert last_review to datetime")
    df['last_review'] = pd.to_datetime(df['last_review'])

    logger.info("Covert dataframe to csv and upload to W&B")
    df.to_csv("clean_sample.csv", index=False)
    artifact = wandb.Artifact(
            name=args.output_artifact,
            type=args.output_type,
            description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Name for the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Name for the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Name fot the type of output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=int,
        help="Lower bound of price",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=int,
        help="Upper bound of price",
        required=True
    )


    args = parser.parse_args()

    go(args)
