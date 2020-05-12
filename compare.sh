#!/bin/bash


cd /work ;

sort $1-res > $1-hidoop ; 
rm $1-res;




cd /home/sfettouh/nosave/hidoop-2/hidoop-2 ;


ts=$(date +%s%N)

java application/Count $1;

te=$(date +%s%N)
 
end=$((($te - $ts)/1000000))

filesize=$(echo $(( $( stat -c '%s' /work/$1 ) / 1024 / 1024 )))

echo -e "------\noperation type : map seq \nfile name : $1 \nfile size : $filesize \nexectime : $end ms\n-----------" >> result

cd /work ;

sort $1-res > $1-seq ; 
rm $1-res ;

echo "diffrences.. in result";


diff $1-seq $1-hidoop ;

