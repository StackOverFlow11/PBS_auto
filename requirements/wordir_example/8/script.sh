#!/bin/bash
#PBS -q medium
#PBS -l nodes=1:ppn=48
#PBS -j oe
#PBS -V
#PBS -o output.o
#PBS -e error.e
NP=`cat $PBS_NODEFILE | wc -l`
cd $PBS_O_WORKDIR
##### definition of job name ################
JOB_NAME=cal
#-------------module-load------------------------------------------
module load intel
module load cp2k-7.1
cp2kexe="/share/apps/cp2k/cp2k-7.1-intel/exe/Linux-x86-64-intel/cp2k.popt"
#----------------------------------------------------------------------
ulimit -s unlimited
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
mpiexec.hydra  -machinefile $PBS_NODEFILE -n $NP $cp2kexe ${INPUT}.inp >${INPUT}.out 2>&1
#############################
echo "END Time:" `date` >> time
date_end=`date "+%s"`
hour=`awk -v y=$date_start -v x=$date_end 'BEGIN {printf "%.2f\n",(x-y)/3600.0}'`
echo "Runing Time(h):" $hour "(h)">> time
done
#-------------module-unload------------------------------------------
module unload intel
module unload cp2k-7.1



