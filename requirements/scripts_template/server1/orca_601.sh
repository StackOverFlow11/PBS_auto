#!/bin/bash
#PBS -q medium
#PBS -l nodes=1:ppn=48
#PBS -j oe
#PBS -V
#PBS -o output.o
#PBS -e error.e
cd $PBS_O_WORKDIR

module load openmpi416
module load ORCA601

#Here giving the path to the ORCA binaries and giving communication protocol
#orcadir=/opt/apps/ORCA_601
#orcaexe=$orcadir/orca
#export PATH=$orcadir:$PATH
#export LD_LIBRARY_PATH=$orcadir:$LD_LIBRARY_PATH

#!!!!!!!  NO need to specify the input file name. ALL input file with suffix as .inp will be submitted AT ONCE.  !!!!!!!
#Start ORCA job. ORCA is started using full pathname (necessary for parallel execution). Output file is written directly to submit directory on frontnode. Temp files are written to ./Temp directory.
PID=1
while [ -d "$GAUSS_SCRDIR/ORCA_$PID" ]
do
    let "PID++"
done
mkdir $GAUSS_SCRDIR/ORCA_$PID
ORCA_SCRDIR=$GAUSS_SCRDIR/ORCA_$PID
for inFile in $PBS_O_WORKDIR/*.inp
do
    Temp=${inFile##*/}
    jobName=${Temp%.*}
    cp $PBS_O_WORKDIR/$jobName.inp $ORCA_SCRDIR/
    if [ -e "$PBS_O_WORKDIR/$jobName.gbw" ]; then cp $PBS_O_WORKDIR/$jobName.gbw $ORCA_SCRDIR/; fi
    /opt/apps/ORCA_601/orca $ORCA_SCRDIR/$jobName.inp > $PBS_O_WORKDIR/$jobName.log &
done
wait

rm -rf $ORCA_SCRDIR/*tmp*
cp $ORCA_SCRDIR/* $PBS_O_WORKDIR/
rm -rf $ORCA_SCRDIR/

#!!!!!!!  If you have several input file in one folder and you only want to submit one of them.  !!!!!!!
#_____________________Comment the section aobve and uncomment this._____________________
#jobName=test
#PID=1
#while [ -d "$GAUSS_SCRDIR/ORCA_$PID" ]
#do
#    let "PID++"
#done
#mkdir $GAUSS_SCRDIR/ORCA_$PID
#ORCA_SCRDIR=$GAUSS_SCRDIR/ORCA_$PID
#cp $PBS_O_WORKDIR/$jobName.inp $ORCA_SCRDIR/
#if [ -e "$PBS_O_WORKDIR/$jobName.gbw" ]; then cp $PBS_O_WORKDIR/$jobName.gbw $ORCA_SCRDIR/; fi

#$orcaexe $ORCA_SCRDIR/$jobName.inp > $PBS_O_WORKDIR/$jobName.log

#rm -rf $ORCA_SCRDIR/*tmp*
#cp $ORCA_SCRDIR/* $PBS_O_WORKDIR/
#rm -rf $ORCA_SCRDIR/
#________________________Remember to specify your input file name________________________

module unload ORCA601
module unload openmpi416
