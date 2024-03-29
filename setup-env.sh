#!/bin/bash
if [ -e /etc/redhat-release ]
then
  sudo dnf -y install python3.9 python3.9-pip binutils make
fi
if [ -e /etc/debian_version ]
then
	sudo apt update
  sudo apt install -y git make binutils python3-venv python3-stdeb fakeroot python3-all dh-python
fi
poetry env use python3.9
poetry install
make dev
