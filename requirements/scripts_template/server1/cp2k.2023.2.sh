#!/bin/bash
#PBS -q long
#PBS -l nodes=1:ppn=96
#PBS -j oe
#PBS -V
#PBS -o output.o
#PBS -e error.e
NP=`cat $PBS_NODEFILE | wc -l`
cd $PBS_O_WORKDIR
##### definition of job name ################
JOB_NAME=cal
#-------------intelmpi+ifort------------------------------------------
module load intel-2023.2
module load devtoolset-11
module load cp2k-2023.2
#----------------------------------------------------------------------
#ulimit -s unlimited

echo "job ${JOB_NAME} starts at `date`" >${JOB_NAME}.out
echo "running on the following nodes, with $NP processes in total" >>${JOB_NAME}.out
cat $PBS_NODEFILE | sort | uniq -c >>${JOB_NAME}.out

#--------------------- cp2k calculation-------------------------------
for file_a in ${PBS_O_WORKDIR}/*.inp; do
echo "Start Time:" `date` > time
date_start=`date "+%s"`
INPUT=`basename $file_a .inp`
FDNAME=`basename $PBS_O_WORKDIR`
####RUN CP2k by INTEL MPI####
mpiexec.hydra  -machinefile $PBS_NODEFILE -n $NP cp2k.popt ${INPUT}.inp >${INPUT}.out 2>&1
#############################
echo "END Time:" `date` >> time
date_end=`date "+%s"`
hour=`awk -v y=$date_start -v x=$date_end 'BEGIN {printf "%.2f\n",(x-y)/3600.0}'`
echo "Runing Time(h):" $hour "(h)">> time
done
#---------------------------------------------------------------------
module unload cp2k-2023.2
module unload devtoolset-11
module unload intel-2023.2
