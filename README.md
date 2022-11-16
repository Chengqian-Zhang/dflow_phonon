# dflow_phonon
Phonon computing workflow based on dflow

## Step1
Prepare a folder with the name **work_dir**.Under this folder, place the files needed for the calculation.If you want to use vasp calculation, you need to prepare **POSCAR**, **POTCAR**, **INCAR**, **param.json**.If you want to use the DP potential function to calculate, you need to prepare **POSCAR**, **param.json**, **frozen_model.pb**
You can see the folder of several demos(**dp_demo**,**vasp_dfpt_demo**,**vasp_displacement_demo**).

## Step2
Fill in your Bohrium **username** and **password** in the **run.py** file.(line **125** and **126**)

## Step3
nohup python3 run.py &

## workflow
You can track the progress of the task at https://workflow.dp.tech/
