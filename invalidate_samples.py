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

sample = "/EmbeddingRun2018C/ElTauFinalState-inputDoubleMu_106X_ULegacy_miniAOD-v2/USER"
filelistpath = "filelist_2018_a.txt"
# first create a dbs3 api instance
dbs_url = "https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter/"
dbs_api = DbsApi(url=dbs_url)
print("Calling DBS API for sample: ", sample)
# get the list of files
# print(f"insert block in DBS3: {dbs_api.url}")
# files = dbs_api.listFiles(dataset=sample, instance="prod/phys03")
# print(files)

dbs_api.updateDatasetType(dataset=sample, dataset_access_type="INVALID")

# # load the filelist
# filelist = []
# with open(filelistpath, "r") as f:
#     for line in f:
#         filelist.append(line.strip())
# print("Loaded filelist with ", len(filelist), " files")
# # now invalidate the files
# # test it with one file first
# # file = filelist[0]
# for file in filelist:
#     print("Invalidating file: ", file)
#     dbs_api.updateFileStatus(logical_file_name=file, is_file_valid=False)