#!/usr/bin/env python
import pprint
import argparse
from dbs.apis.dbsClient import DbsApi
from rich import progress as rich_progress
import io, sys, os
import subprocess
import time
import json
import pdb
import numpy as np
from multiprocessing import Manager
from concurrent.futures import as_completed, ProcessPoolExecutor
import ROOT


def getlumi(file):
    output = subprocess.check_output("edmLumisInFiles.py {}".format(file), shell=True)
    lumidict = json.loads(output)
    outputdata = []
    for run in lumidict:
        for lumirange in lumidict[run]:
            lumilow = lumirange[0]
            lumihigh = lumirange[1]
            for lumi in range(lumilow, lumihigh + 1):
                outputdata.append({"lumi_section_num": lumi, "run_num": int(run)})
    return outputdata


def get_file_information(filepath):
    # print("Reading file: %s" % filepath)
    ROOT.gErrorIgnoreLevel = 6001
    file = ROOT.TFile.Open(filepath, "READ")
    tree = file.Get("Events")
    data = {}
    data["nentries"] = int(tree.GetEntries())
    data["check_sum"] = "NULL"
    data["lumis"] = getlumi(filepath)
    return data


def job_wrapper(args):
    return readout_file_information(*args)


def readout_file_information(
    progress,
    tasknumber,
    block,
    files,
    outputfolder,
):
    outputfile = os.path.join(
        outputfolder, "temp", "file_details_{}.json".format(tasknumber)
    )
    if os.path.exists(outputfile):
        print("File {} already exists".format(outputfile))
        return
    data = {}
    output_data = []
    for i, rootfile_info in enumerate(files):
        fileinfo = get_file_information(
            "root://xrootd-cms.infn.it//" + rootfile_info["name"]
        )
        aFile = {
            "name": rootfile_info["name"],
            "event_count": fileinfo["nentries"],
            "file_size": rootfile_info["bytes"],
            "check_sum": "NULL",
            "guid": rootfile_info["guid"],
            "lumis": fileinfo["lumis"],
            "adler32": rootfile_info["adler32"],
        }
        output_data.append(aFile)
        # bar.update(1)
        progress[tasknumber] = {"progress": i, "total": len(files)}
    #         # bar.update(1)
    data[tasknumber] = output_data
    json.dump(data, open(outputfile, "w"))


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Script to generate json for DBS3 upload"
    )
    parser.add_argument("--era", type=str, help="era", required=True)
    parser.add_argument(
        "--processed-ds", type=str, help="Second part of the dbs nick", required=True
    )
    parser.add_argument(
        "--primary-ds", type=str, help="First part of the dbs nick", required=True
    )
    parser.add_argument(
        "--publish-config",
        type=str,
        required=True,
        help="path to the configuration settings for publishing datasets",
        default="",
    )
    parser.add_argument(
        "--rucio-input",
        type=str,
        help="The input file extracted from rucio",
        required=True,
    )
    parser.add_argument(
        "--task",
        type=str,
        required=True,
        choices=["prepare", "publish", "test_publish"],
        help="Name the task to be performed",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=12,
        help="Number of threads to be used",
    )
    return parser.parse_args()


class DBS3Uploader(object):
    def __init__(self, era, dbs_nick, publish_config, rucio_config, threads):
        self.era = era
        self.processed_ds = dbs_nick.split("/")[2]
        self.primary_ds = dbs_nick.split("/")[1]
        self.publish_config = publish_config
        self.rucio_config = rucio_config
        self.threads = threads
        self.dbs_url = "https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter/"
        self.dbs_api = DbsApi(url=self.dbs_url)
        # setup the basics
        # first create the folders
        sample = "{}_{}_{}".format(
            self.era,
            self.primary_ds,
            self.processed_ds,
        )
        self.sample_publish_folder = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "workdir_publishing", sample
        )
        if not os.path.exists(self.sample_publish_folder):
            os.makedirs(self.sample_publish_folder)
        # now locate the correct entry, matching era, name and run
        self.entry = self.get_entry_from_config(
            self.publish_config, self.era, self.primary_ds, self.processed_ds
        )
        self.origin_site_name = self.entry["origin_site_name"]
        # now match the entry with the information from the rucio information
        self.rucio_entry = self.get_entry_from_rucio(
            self.rucio_config, self.entry["dbs_name"]
        )

        # create the dataset_info.json file if not existent
        self.dataset_info_file = os.path.join(
            self.sample_publish_folder, "dataset_info.json"
        )
        self.file_info_file = os.path.join(self.sample_publish_folder, "file_info.json")

        if not os.path.exists(self.dataset_info_file):
            self.generate_dataset_info(self.entry, self.dataset_info_file)

    def prepare(self):
        # now create the dataset_info.json file
        print("Preparing dataset")
        if not os.path.exists(
            os.path.join(self.sample_publish_folder, "ready_for_publish")
        ):
            # now we have to parse all the different files and create the block json
            self.generate_file_json(
                self.file_info_file,
                self.rucio_entry,
                self.sample_publish_folder,
                nthreads=self.threads,
            )
        else:
            print("Dataset already prepared")

    def publish_test(self):
        # now we have to upload the dataset to DBS3
        if not os.path.exists(
            os.path.join(self.sample_publish_folder, "ready_for_publish")
        ):
            print("Dataset not prepared yet")
            exit()
        else:
            print("Uploading dataset to DBS3")
            self.upload_to_dbs(
                self.dataset_info_file,
                self.file_info_file,
                self.origin_site_name,
                dry=True,
            )

    def publish(self):
        # now we have to upload the dataset to DBS3
        if not os.path.exists(
            os.path.join(self.sample_publish_folder, "ready_for_publish")
        ):
            print("Dataset not prepared yet")
            exit()
        else:
            print("Uploading dataset to DBS3")
            self.upload_to_dbs(
                self.dataset_info_file,
                self.file_info_file,
                self.origin_site_name,
                dry=False,
            )

    def create_new_block(self, ds_info, origin_site_name, blockid):

        acquisition_era_config = {"acquisition_era_name": "CRAB", "start_date": 0}
        processing_era_config = {
            "processing_version": 1,
            "description": ds_info["description"],
        }
        primds_config = {
            "primary_ds_type": ds_info["primary_ds_type"],
            "primary_ds_name": ds_info["primary_ds"],
        }

        dataset = "/%s/%s/%s" % (
            ds_info["primary_ds"],
            ds_info["processed_ds"],
            ds_info["tier"],
        )

        dataset_config = {
            "physics_group_name": ds_info["group"],
            "dataset_access_type": "VALID",
            "data_tier_name": ds_info["tier"],
            "processed_ds_name": ds_info["processed_ds"],
            "dataset": dataset,
        }

        block_name = "%s#%s" % (dataset, blockid)
        block_config = {
            "block_name": block_name,
            "origin_site_name": origin_site_name,
            "open_for_writing": 0,
        }

        dataset_conf_list = [
            {
                "app_name": ds_info["application"],
                "global_tag": ds_info["global_tag"],
                "output_module_label": ds_info["output_module_label"],
                "pset_hash": "dummyhash",
                "release_version": ds_info["app_version"],
            }
        ]

        blockDict = {
            "files": [],
            "processing_era": processing_era_config,
            "primds": primds_config,
            "dataset": dataset_config,
            "dataset_conf_list": dataset_conf_list,
            "acquisition_era": acquisition_era_config,
            "block": block_config,
            "file_parent_list": [],
            "file_conf_list": [],
        }
        return blockDict

    def add_files_to_block(self, blockDict, files):
        blockDict["files"] = files
        blockDict["block"]["file_count"] = len(files)
        blockDict["block"]["block_size"] = sum(
            [int(file["file_size"]) for file in files]
        )
        return blockDict

    def generate_dataset_info(self, config, outputfile):
        # INFORMATION TO BE PUT IN DBS3
        # almost free text here, but beware WMCore/Lexicon.py
        dataset_info = {
            "primary_ds": config["primary_ds"],
            "processed_ds": config["processed_ds"],
            "tier": config["tier"],
            "group": config["group"],
            "campaign_name": config["campaign_name"],
            "application": config["application"],
            "app_version": config["app_version"],
            "description": config["description"],
            "primary_ds_type": config["primary_ds_type"],
            "global_tag": config["global_tag"],
            "output_module_label": config["output_module_label"],
        }
        if config["parent_dataset"] is not None:
            dataset_info["parent_dataset"] = config["parent_dataset"]
        else:
            dataset_info["parent_dataset"] = "None"
        json.dump(dataset_info, open(outputfile, "w"))

    def generate_file_json(self, file_info_file, rucio_entry, outputfolder, nthreads):
        blocks = [block["blockname"] for block in rucio_entry]
        ntasks = len(blocks)
        arguments = [
            (
                blocks[i],
                rucio_entry[i]["files"],
                outputfolder,
                i,
            )
            for i in range(ntasks)
        ]
        # create temp folder for the output
        if not os.path.exists(os.path.join(outputfolder, "temp")):
            os.makedirs(os.path.join(outputfolder, "temp"))
        if nthreads > len(arguments):
            nthreads = len(arguments)
        print("Running {} tasks with {} threads".format(len(arguments), nthreads))
        with rich_progress.Progress(
            "[progress.description]{task.description}",
            rich_progress.BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            rich_progress.TimeRemainingColumn(),
            rich_progress.TimeElapsedColumn(),
            refresh_per_second=10,  # bit slower updates
        ) as progress:
            futures = []  # keep track of the jobs
            with Manager() as manager:
                # this is the key - we share some state between our
                # main process and our worker functions
                _progress = manager.dict()
                overall_progress_task = progress.add_task("[green]All tasks progress:")

                with ProcessPoolExecutor(max_workers=nthreads) as executor:
                    for (
                        block,
                        files,
                        outputfolder,
                        tasknumber,
                    ) in arguments:  # iterate over the jobs we need to run
                        # set visible false so we don't have a lot of bars all at once:
                        task_id = progress.add_task(f"task {tasknumber}", visible=False)
                        futures.append(
                            executor.submit(
                                readout_file_information,
                                _progress,
                                tasknumber,
                                block,
                                files,
                                outputfolder,
                            )
                        )

                    # monitor the progress:
                    while (
                        n_finished := sum([future.done() for future in futures])
                    ) < len(futures):
                        progress.update(
                            overall_progress_task,
                            completed=n_finished,
                            total=len(futures),
                        )
                        for task_id, update_data in _progress.items():
                            latest = update_data["progress"]
                            total = update_data["total"]
                            # update the progress bar for this task:
                            progress.update(
                                task_id,
                                completed=latest,
                                total=total,
                                visible=latest < total,
                            )

                    # raise any errors:
                    for future in futures:
                        future.result()
        print("Done generating file information")
        # merge all json files into a single one
        combined = {}
        combined["blocks"] = []
        for i in range(ntasks):
            result = json.loads(
                open(
                    os.path.join(
                        outputfolder, "temp", "file_details_{}.json".format(i)
                    ),
                    "r",
                ).read()
            )
            block = {
                "blockid": blocks[i].split("#")[1],
                "files": result[str(i)],
            }
            combined["blocks"].append(block)
        json.dump(combined, open(file_info_file, "w"), indent=4)
        print("Finished preparing dbs for {} blocks...".format(len(combined["blocks"])))
        # generate file indicating that the filelist is complete
        open(os.path.join(outputfolder, "ready_for_publish"), "a").close()

    def upload_to_dbs(
        self,
        dataset_info_file,
        file_info_file,
        origin_site_name,
        dry=False,
        max_retries=5,
    ):
        if dry:
            print("Dry run, preparing DBS3 upload...")
        else:
            print("Uploading to DBS3...")
        with open(dataset_info_file, "r") as f:
            dataset_info = json.loads(f.read())
        with open(file_info_file, "r") as f:
            file_info = json.loads(f.read())
        total_files = 0
        total_events = 0
        print(f"insert block in DBS3: {self.dbs_api.url}")
        print("Preparing upload for {}".format(dataset_info["processed_ds"]))
        print("Blocks to be processed: {}".format(len(file_info["blocks"])))
        print(
            "DatasetName: {}".format(
                self.create_new_block(dataset_info, origin_site_name, "asdf")[
                    "dataset"
                ]["dataset"]
            )
        )
        failed_blocks = []
        for block in file_info["blocks"]:
            blockid = block["blockid"]
            filedata = block["files"]
            filelist = []
            print(
                "Processing block {} - Number of files: {}".format(
                    blockid, len(filedata)
                )
            )
            total_files += len(filedata)
            blockDict = self.create_new_block(dataset_info, origin_site_name, blockid)
            for file in filedata:
                fileDic = {}
                lfn = file["name"]
                fileDic["file_type"] = "EDM"
                fileDic["logical_file_name"] = lfn
                for key in ["check_sum", "adler32", "file_size", "event_count", "guid"]:
                    fileDic[key] = file[key]
                total_events += file["event_count"]
                fileDic["file_lumi_list"] = file["lumis"]
                fileDic["auto_cross_section"] = 0.0
                fileDic["last_modified_by"] = os.getlogin()
                filelist.append(fileDic)
            # now upload the block
            blockDict = self.add_files_to_block(blockDict, filelist)
            tries = 0
            uploaded = False
            if not dry:
                while tries < max_retries and uploaded is False:
                    text_trap = io.StringIO()
                    try:
                        # this is a hack to suppress the output of the DBS3 API
                        sys.stdout = text_trap
                        self.dbs_api.insertBulkBlock(blockDict)
                        sys.stdout = sys.__stdout__
                        uploaded = True
                    except Exception as e:
                        sys.stdout = sys.__stdout__
                        if "Data already exist in DBS Error" in str(e):
                            print("Block already exists in DBS3, skipping...")
                            uploaded = True
                        else:
                            failed_blocks.append(
                                blockid
                            ) if blockid not in failed_blocks else None
                            tries += 1
                            print("Error while uploading block: {}".format(blockid))
                            print(e)
                            if (
                                "Message:Data already exist in DBS Error"
                                not in text_trap.getvalue()
                            ):
                                print(text_trap.getvalue())
                            print("Retrying in 5 seconds...")
                            time.sleep(5)
            else:
                print("Dry run, not inserting block into DBS3")
                # pprint.pprint(blockDict)
                exit()
        print("Total files: {} // Total Events: {}".format(total_files, total_events))
        print(f"{len(failed_blocks)} blocks failed: {failed_blocks}")

    def get_entry_from_config(self, config, era, primary_ds, processed_ds):
        """
        Get the entry from the config that matches the given era, name and primary_ds. If there are multiple matches or no match, exit.
        """
        result = []
        for entry in config.keys():
            if (
                config[entry]["era"] == int(era)
                and config[entry]["processed_ds"] == processed_ds
                and config[entry]["primary_ds"] == primary_ds
            ):
                config[entry]["dbs_name"] = entry
                result.append(config[entry])
        if len(result) == 0:
            print(
                "Could not find entry in config for {} {} {}".format(
                    era, processed_ds, primary_ds
                )
            )
            exit()
        elif len(result) > 1:
            print(
                "Found multiple entries in config for {} {} {}".format(
                    era, processed_ds, primary_ds
                )
            )
            print(result)
            exit()
        else:
            return result[0]

    def get_entry_from_rucio(self, config, dbs_name):
        """
        Get the entry from the config that matches the given dbs_name. If there are multiple matches or no match, exit.
        """
        result = []
        for entry in config.keys():
            if entry == dbs_name:
                result.append(config[entry])
        if len(result) == 0:
            print("Could not find entry in config for {}".format(dbs_name))
            exit()
        elif len(result) > 1:
            print("Found multiple entries in config for {}".format(dbs_name))
            print(result)
            exit()
        else:
            return result[0]
