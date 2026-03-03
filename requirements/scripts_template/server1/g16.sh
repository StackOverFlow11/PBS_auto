#!/bin/bash
#PBS -q long
#PBS -l nodes=1:ppn=24
############################################### 
#      Notice: For this G16 version           #
#     max_nodes=1 & max_cores(ppn)=64         #
#more than 1 node or 64 ppn will cause error! #
###############################################
#PBS -j oe
#PBS -l mem=50gb
#PBS -V
#PBS -o output.o
#PBS -e error.e
NP=`cat $PBS_NODEFILE | wc -l`
cd $PBS_O_WORKDIR
##### definition of job name ################
JOB_NAME=cal

module load g16
echo "job ${JOB_NAME} starts at `date`" >${JOB_NAME}.out
echo "running on the following nodes, with $NP processes in total" >>${JOB_NAME}.out
cat $PBS_NODEFILE | sort | uniq -c >>${JOB_NAME}.out

#--------------------- g16 calculation-------------------------------
for file_a in ${PBS_O_WORKDIR}/*.gjf; do
echo "Start Time:" `date` > time
date_start=`date "+%s"`
INPUT=`basename $file_a .gjf`
FDNAME=`basename $PBS_O_WORKDIR`
####RUN Gaussian 16 ####
g16 <${INPUT}.gjf> ${INPUT}.log 2>&1
########################
echo "END Time:" `date` >> time
date_end=`date "+%s"`
hour=`awk -v y=$date_start -v x=$date_end 'BEGIN {printf "%.2f\n",(x-y)/3600.0}'`
echo "Runing Time(h):" $hour "(h)">> time
done
#---------------------------------------------------------------------

module unload g16

