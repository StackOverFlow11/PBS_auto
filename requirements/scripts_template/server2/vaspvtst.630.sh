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
#echo -n "start time  " > time

#-------------module-load------------------------------------------
module load intel
module load vasp-vtst
#####choose your vasp version
vasp_gam="/share/apps/vasp/vasp.6.3.0-vtst/bin/vasp_gam_vtst"
vasp_std="/share/apps/vasp/vasp.6.3.0-vtst/bin/vasp_std_vtst"
#----------------------------------------------------------------------

echo "job ${JOB_NAME} starts at `date`" >${JOB_NAME}.out
echo "running on the following nodes, with $NP processes in total" >>${JOB_NAME}.out
cat $PBS_NODEFILE | sort | uniq -c >>${JOB_NAME}.out

#--------------------- vasp calculation-------------------------------
echo "Start Time:" `date` > time
date_start=`date "+%s"`
FDNAME=`basename $PBS_O_WORKDIR`
##########RUN VASP BY INTEL MPI##########
####CHOOSE vasp_std or vasp_gam!!!!!#####
mpiexec.hydra  -machinefile $PBS_NODEFILE -n $NP $vasp_std > stdout 2>&1
#########################################
echo "END Time:" `date` >> time
date_end=`date "+%s"`
hour=`awk -v y=$date_start -v x=$date_end 'BEGIN {printf "%.2f\n",(x-y)/3600.0}'`
echo "Runing Time(h):" $hour "(h)">> time
#---------------------------------------------------------------------
module unload vasp-vtst
module unload intel



