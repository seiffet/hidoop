#!/bin/bash


cd /home/sfettouh/nosave/hidoop-2/hidoop-2 ;

javac */*.java ;

bash fichierconfig.sh;




bash writefile.sh $1;


ts=$(date +%s%N)

java application/MyMapReduce $1

te=$(date +%s%N)

 
end=$((($te - $ts)/1000000))

filesize=$(echo $(( $( stat -c '%s' /work/$1 ) / 1024 / 1024 )))

echo -e "------\noperation type : map + read + reduce \nfile name : $1 \nfile size : $filesize \nexectime : $end ms\n-----------" >> result


bash compare.sh $1;
