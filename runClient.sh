#!/bin/bash
# Java Script

if [ "$#" -eq 0 ]; then
  echo "Running ATL"
  java com.company.Main
fi

if [ "$#" -eq 1 ]; then
  echo "Running $1"
  java com.company.Main -a $1
fi
