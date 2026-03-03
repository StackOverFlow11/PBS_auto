#!/bin/bash
#PBS -q medium
#PBS -l nodes=1:ppn=48
#PBS -j oe
#PBS -V
#PBS -o output.o
#PBS -e error.e
module load openmpi314
module load ORCA421
cd $PBS_O_WORKDIR

#Setting OPENMPI paths here:
#ompidir=/opt/apps/openmpi-3.1.4
#export PATH=$ompidir/bin:$PATH
#export LD_LIBRARY_PATH=$ompidir/lib:$LD_LIBRARY_PATH

#Here giving the path to the ORCA binaries and giving communication protocol
#orcadir=/opt/apps/ORCA_421
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
    orca $ORCA_SCRDIR/$jobName.inp > $PBS_O_WORKDIR/$jobName.log &
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
module unload ORCA421
module unload openmpi314
