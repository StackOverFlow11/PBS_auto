#!/bin/bash
#PBS -q medium
#PBS -l nodes=1:ppn=24,mem=80gb
#PBS -j oe
#PBS -V
#PBS -o output.o
#PBS -e error.e
NP=`cat $PBS_NODEFILE | wc -l`
cd $PBS_O_WORKDIR
##### definition of job name ################
JOB_NAME=cal
#echo -n "start time  " > time

#-------------intelmpi+ifort------------------------------------------
#source /opt/apps/intel2017u8/compilers_and_libraries_2017.8.262/linux/bin/ifortvars.sh intel64
#source /opt/apps/intel2017u8/compilers_and_libraries_2017.8.262/linux/bin/iccvars.sh intel64
#source /opt/apps/intel2017u8/compilers_and_libraries_2017.8.262/linux/mkl/bin/mklvars.sh intel64
#source /opt/apps/intel2017u8/compilers_and_libraries_2017.8.262/linux/mpi/intel64/bin/mpivars.sh intel64
#----------------------------------------------------------------------
module load intel2018

echo "job ${JOB_NAME} starts at `date`" >${JOB_NAME}.out
echo "running on the following nodes, with $NP processes in total" >>${JOB_NAME}.out
cat $PBS_NODEFILE | sort | uniq -c >>${JOB_NAME}.out

#--------------------- g16 calculation-------------------------------
echo "Start Time:" `date` > time
date_start=`date "+%s"`
####RUN ABCluster geom####
OMP_NUM_THREADS=1
INPUT=XXXX
geom $INPUT".inp" > $INPUT".out"
#can change geom to other module
########################
echo "END Time:" `date` >> time
date_end=`date "+%s"`
hour=`awk -v y=$date_start -v x=$date_end 'BEGIN {printf "%.2f\n",(x-y)/3600.0}'`
echo "Runing Time(h):" $hour "(h)">> time
#---------------------------------------------------------------------

module unload intel2018
