#!/usr/bin/env bash


apt update

apt -y install dpkg python3-pip

pip3 install wget

python3 deploy.py