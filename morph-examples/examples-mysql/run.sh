#!/bin/bash
FILES=*.sh

for f in $FILES
do 

	#echo "file : $f"
	if [ $f != $0 ]; 
	then
             sh $f
	fi
done

