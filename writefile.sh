#!/bin/bash





#execttime = $(time ssh darwin -t "cd nosave/hidoop-2/hidoop-2 ;java application/WriteFile $1") 

cd /home/sfettouh/nosave/hidoop-2/hidoop-2 ;

ts=$(date +%s%N)

java application/WriteFile $1;

te=$(date +%s%N)

 
end=$((($te - $ts)/1000000))

filesize=$(echo $(( $( stat -c '%s' /work/$1 ) / 1024 / 1024 )))

echo -e "------\noperation type : write \nfile name : $1 \nfile size : $filesize \nexectime : $end ms\n-----------" >> result
