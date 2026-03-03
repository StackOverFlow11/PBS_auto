#!/bin/bash
#PBS -q long
#PBS -l nodes=1:ppn=24
#PBS -j oe
#PBS -V
#PBS -o output.o
#PBS -e error.e
NP=`cat $PBS_NODEFILE | wc -l`
cd $PBS_O_WORKDIR
##### definition of job name ################

#define the jobName
jobName=Rh2
export MOLPRO_OPTIONS="-n $NP -s --no-xml-output -m 512m -d $GAUSS_SCRDIR"
#--------------------- molpro calculation-------------------------------
echo "Start Time:" `date` > time
date_start=`date "+%s"`
FDNAME=`basename $PBS_O_WORKDIR`
molpro < $jobName.inp > $jobName.log 2>&1
echo "END Time:" `date` >> time
date_end=`date "+%s"`
hour=`awk -v y=$date_start -v x=$date_end 'BEGIN {printf "%.2f\n",(x-y)/3600.0}'`
echo "Runing Time(h):" $hour "(h)">> time
#---------------------------------------------------------------------



