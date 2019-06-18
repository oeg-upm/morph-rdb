#!/bin/bash
FILES=*.sh

for f in $FILES
do 


	if [ $f != $0 ] || [$f != "compose.sh" ]; 
	then
        echo "executing file : $f ...\n"
        sh $f
	fi
done

