
# Here giving the path to the Gaussian 09 binaries
export g09root=/opt/apps/Gaussian
export g16root=/opt/apps/Gaussian
source $g09root/g09/bsd/g09.profile
source $g16root/g16/bsd/g16.profile

# Here giving the path to the TGMin2 binaries
source /opt/apps/anaconda3/bin/activate TGMIN
export PYTHONPATH=/opt/apps/TGMin2:$PYTHONPATH
export PYTHONPATH=/opt/apps/anaconda3/envs/TGMIN/lib/python2.7/site-packages:$PYTHONPATH

#Start TGMin2 job.
jobName=1-2-La2B18
python /opt/apps/TGMin2/run.py $jobName.job
