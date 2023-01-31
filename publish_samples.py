from rucio.client.client import Client
import os
import json
import yaml
from prepare_dbs_data import DBS3Uploader
from rucio_helper import get_account_rules, get_rucio_blocks, setup_rucio_account
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "--mode", help="Mode to run the script in", choices=["rucio", "prepare", "publish"]
)
parser.add_argument("--rucio-db-file", help="Rucio DB file", default="rucio_data.json")
parser.add_argument(
    "--dbs-config-file", help="DBS config file", default="dbs_config.yml"
)

args = parser.parse_args()
# set environment variable RUCIO_CONFIG
os.environ["RUCIO_CONFIG"] = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "rucio.cfg"
)
# defaults
account = "pog_tau_group"
scope = "group.pog_tau_group"
dbs_regex = "/.*./.*.inputDoubleMu_106X_ULegacy_miniAOD.*./USER"
rucio_db_file = args.rucio_db_file
publish_config = args.dbs_config_file


if args.mode == "rucio":
    # First step: get the sample information from Rucio
    print("Getting sample information from Rucio")
    setup_rucio_account()
    client = Client()
    transfered_samples = get_account_rules(client, account, dbs_regex)
    data = {}
    for sample in transfered_samples:
        rucio_data = get_rucio_blocks(client, scope, account, sample["name"])
        data[sample["name"]] = rucio_data
        print(f"Sample: {sample['name']}")
        print(f"Number of blocks: {len(rucio_data)}")
        print(
            f"Number of files: {sum([len(rucio_data[x]['files']) for x in range(len(rucio_data))])}"
        )
    # dump data into json file
    json.dump(data, open(rucio_db_file, "w"), indent=4)
else:
    # load data from json file
    rucio_db = json.load(open(rucio_db_file, "r"))
    # Now, per sample create one DBS3Uploader object, prepare the data, and upload it to DBS
    # load the publish db file
    publish_config = yaml.safe_load(open(publish_config, "r"))
    for sample in publish_config:
        if sample not in rucio_db:
            print(f"Sample {sample} not found in rucio data")
            continue
        era = publish_config[sample]["era"]
        print(f"Processing sample {sample}")
        dbs_uploader = DBS3Uploader(era, sample, publish_config, rucio_db, 20)
        if args.mode == "prepare":
            dbs_uploader.prepare()
        elif args.mode == "publish":
            dbs_uploader.publish()

