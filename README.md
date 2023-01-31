# publish_dbs3_samples
Collection of scripts to publish samples in dbs phys03 instance based on rucio information
# Setup the tool

```bash
git clone git@github.com:KIT-CMS/publish_dbs3_samples.git
# setup the rucio client
source /cvmfs/cms.cern.ch/rucio/setup-py3.sh
# setup CMSSW 12_6_0
scram project CMSSW_12_6_0
cd CMSSW_12_6_0/src
cmsenv
cd -
```