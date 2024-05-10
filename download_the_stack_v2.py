import os
import argparse
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import smart_open
from datasets import load_dataset

def download_the_stack_v2(data_repo, language, hug_access_token, download_folder):
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    ds = load_dataset(data_repo, language, split="train", streaming=True, token=hug_access_token)
    # uncomment below line for the scenario of distributed data download.
    #ds = ds.filter(lambda row, idx: idx % worker_num == worker_id, with_indices=True)
    for i, row in enumerate(ds):
        blob_id, src_encoding = row["blob_id"], row["src_encoding"]
        s3_url = f"s3://softwareheritage/content/{blob_id}"
        with smart_open.open(s3_url, "rb", compression=".gz", transport_params={"client": s3}) as fin:
            content = fin.read().decode(src_encoding)
        # replace below code snippet for your specific storage logic.
        data_path = os.path.join(download_folder, blob_id)
        with open(data_path, "w") as f:
            f.write(content)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="the-stack-v2 download entry.")
    parser.add_argument("--data_repo", type=str, default="bigcode/the-stack-v2", help="The data repo name.")
    parser.add_argument("--language", type=str, default="Python", help="The programming language name, None is the whole dataset.")
    parser.add_argument("--hug_access_token", type=str, default="your_huggingface_access_token", help="The access token of huggingface account, which could be acquired from https://huggingface.co/settings/tokens.")
    parser.add_argument("--download_folder", type=str, default=".", help="The folder path to download the data.")
    args = parser.parse_args()

    download_the_stack_v2(args.data_repo, args.language, args.hug_access_token, args.download_folder)
