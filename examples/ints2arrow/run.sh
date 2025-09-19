#!/bin/sh

printf '\0\1\2\3' > sample1.i16.be.dat
printf '\1\0\3\2' > sample2.i16.le.dat

ENV_INPUT_RAW_INTS_16_BE=./sample1.i16.be.dat ./ints2arrow
ENV_INPUT_RAW_INTS_16_BE=./sample2.i16.le.dat ./ints2arrow
