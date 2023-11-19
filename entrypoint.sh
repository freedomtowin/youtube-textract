#!/bin/bash

if [ "prd" == "${BUILD_ENV}" ]; then  
	python3 app.py
else
	jupyter notebook --ip 00.0 --port 9999 --NotebookApp.token='' --NotebookApp.password='' --no-browser --allow-root
	sleep infinity
fi 