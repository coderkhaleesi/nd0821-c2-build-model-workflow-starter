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

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    local_path = wandb.use_artifact(args.input_artifact).file()
    df = pd.read_csv(local_path)

    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    df['last_review'] = pd.to_datetime(df['last_review'])

    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    logger.info("Saving artifact")
    df.to_csv(args.output_artifact, index=False)

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description
    )

    artifact.add_file(args.output_artifact)

    run.log_artifact(artifact)
    logger.info("Clean data saved")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Basic data cleaning job")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Name fo the input artifact from wandb",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Output file where you want to save output data",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price as per stakeholders",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price as per stakeholders",
        required=True
    )


    args = parser.parse_args()

    go(args)
