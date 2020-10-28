#!/bin/bash
./arangokvload \
--time 20 --threads 8 --batchsize 1000 --numdocs 1000000 --numattrs 100 --attrlen 10 --pctwrite 20 \
--user root --password "" 
