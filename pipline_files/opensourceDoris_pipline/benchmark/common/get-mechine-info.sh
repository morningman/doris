#!/bin/bash

set -e
set -o pipefail

echo -e "\n\n\n\n
#############################
host info
hostname
#############################
"
hostname

echo -e "\n\n\n\n
#############################
cpu info
lscpu
#############################
"
lscpu

echo -e "\n\n\n\n
#############################
DMI info
sudo dmidecode
#############################
"
sudo dmidecode
