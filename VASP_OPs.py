from dflow import config, s3_config
config["host"] = "https://workflow.dp.tech/"
s3_config["endpoint"] = "60.205.112.9:9000"
config["k8s_api_server"] = "https://182.92.168.135:6443"
config["token"] = "eyJhbGciOiJSUzI1NiIsImtpZCI6Im05a2kzYm1TUEhHVWxoeTk1ZVExNHBrZnBhM3FYbVFHb3VMU2d3NzM1NUEifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJhcmdvIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6ImFyZ28tdG9rZW4tcnZ2d2QiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC5uYW1lIjoiYXJnbyIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50LnVpZCI6IjgwN2I4NzMzLTQwMTAtNDc0NC1hNDQxLTkzZGNjMTgwNzQyZCIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDphcmdvOmFyZ28ifQ.NwpdoIBVg9GV2nQS24Cxrotg__MXlSsngjmpy7wcLhlHaHIwJYQFqCstDr2fpakzJOEAPxdn87SR4rjSSipecftUgE0IyBlKFcFx-sRlO0KHfFOQ83fcGJXe28N5qD1CWzgQqV9-s-WVWGBsmHcN2O4kxjfcfWUsOFspcoUFIw1ihuxGl5dj1fTJ6qtezChljB_gzGvUlwoJ_8oex3iT-BT_o9dJpBOTMtK0y93AJt4udLVbLUsYSjmQDrckx5nlHT6v1bQ-4K_8DAWNYXvzPwkGzTPhpO_zt-osBsvbjwAJViCbzklN27yU9eO0xfasC_8meefNt-LLq1wS59YeLg"

import json,pathlib
from typing import List
from dflow import (
    Workflow,
    Step,
    argo_range,
    SlurmRemoteExecutor,
    upload_artifact,
    download_artifact,
    InputArtifact,
    OutputArtifact,
    ShellOPTemplate
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Artifact,
    Slices,
    upload_packages
)
import time

import subprocess, os, shutil, glob,dpdata
from pathlib import Path
from typing import List
from monty.serialization import loadfn
from dflow.plugins.bohrium import BohriumContext, BohriumExecutor
from dpdata.periodic_table import Element
from monty.serialization import loadfn

from dflow.python import upload_packages
upload_packages.append(__file__)

class PhononMakeVASP(OP):
    """
    class for making calculation tasks
    """
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'input': Artifact(Path)
        })
 
    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'output' : Artifact(Path),
            'njobs':int, 
            'jobs': Artifact(List[Path])
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        cwd = os.getcwd()
        for i in range(100):
            print("cwd",cwd)
        for i in range(100):
            print("listdir",os.listdir())

        os.chdir(op_in["input"])
        work_d = os.getcwd()

        parameter = loadfn("param.json")["properties"]
        inter_param_prop = loadfn("param.json")["interaction"]
        parameter['primitive'] = parameter.get('primitive', False)
        parameter['approach'] =parameter.get('approach', "linear")
        band_path = parameter['band_path']
        supercell_matrix = parameter['supercell_matrix']
        primitive = parameter['primitive']
        approach = parameter['approach']

        if primitive:
            subprocess.call('phonopy --symmetry',shell=True)
            subprocess.call('cp PPOSCAR POSCAR',shell=True)
            shutil.copyfile("PPOSCAR","POSCAR-unitcell")
        else:
            shutil.copyfile("POSCAR","POSCAR-unitcell")
        
        with open("POSCAR","r") as fp:
            lines = fp.read().split('\n')
            ele_list = lines[5].split()

        cmd = "phonopy -d --dim='%d %d %d' -c POSCAR"%(int(supercell_matrix[0]),int(supercell_matrix[1]),int(supercell_matrix[2]))
        subprocess.call(cmd,shell=True)

        task_list = []

        if(approach == "linear"):
            output_task = os.path.join(work_d,'task.000000')
            os.makedirs(output_task,exist_ok = True)
            os.chdir(output_task)
            for jj in ['INCAR', 'POTCAR', 'POSCAR', 'conf.lmp', 'in.lammps','POSCAR-unitcell','SPOSCAR']:
                if os.path.exists(jj):
                    os.remove(jj)
            task_list.append(output_task)
            os.symlink(os.path.join(work_d,"SPOSCAR"),"POSCAR")
            os.symlink(os.path.join(work_d,"POSCAR-unitcell"),"POSCAR-unitcell")
            os.symlink(os.path.join(work_d,"INCAR"),"INCAR")
            os.symlink(os.path.join(work_d,"POTCAR"),"POTCAR")
            os.symlink(os.path.join(work_d,"param.json"),"param.json")
            with open('band.conf','w') as fp:
                fp.write('ATOM_NAME = ')
                for ii in ele_list:
                    fp.write(ii)
                    fp.write(' ')
                fp.write('\n')
                fp.write('DIM = %s %s %s\n'%(supercell_matrix[0],supercell_matrix[1],supercell_matrix[2]))
                fp.write('BAND = %s\n'%band_path)
                fp.write('FORCE_CONSTANTS=READ\n')
        
        elif(approach == "displacement"):
            poscar_list = glob.glob("POSCAR-0*")
            n_task = len(poscar_list)
            for ii in range(n_task):
                output_task = os.path.join(work_d,'task.%06d' % ii)
                os.makedirs(output_task,exist_ok=True)
                os.chdir(output_task)
                for jj in ['INCAR', 'POTCAR', 'POSCAR', 'conf.lmp', 'in.lammps','POSCAR-unitcell','SPOSCAR']:
                    if os.path.exists(jj):
                        os.remove(jj)
                task_list.append(output_task)
                os.symlink(os.path.join(work_d,poscar_list[ii]), 'POSCAR')
                os.symlink(os.path.join(work_d,"PPOSCAR"),"POSCAR-unitcell")
                os.symlink(os.path.join(work_d,"INCAR"),"INCAR")
                os.symlink(os.path.join(work_d,"POTCAR"),"POTCAR")
                os.symlink(os.path.join(work_d,"param.json"),"param.json")

            os.chdir("../")
            with open('band.conf','w') as fp:
                fp.write('ATOM_NAME = ')
                for ii in ele_list:
                    fp.write(ii)
                    fp.write(' ')
                fp.write('\n')
                fp.write('DIM = %s %s %s\n'%(supercell_matrix[0],supercell_matrix[1],supercell_matrix[2]))
                fp.write('BAND = %s\n'%band_path)

            shutil.copyfile("band.conf","task.000000/band.conf")
            shutil.copyfile("phonopy_disp.yaml","task.000000/phonopy_disp.yaml")

        jobss = glob.glob(os.path.join(work_d,"task*"))
        njobs = len(jobss)
        jobs = []
        for job in jobss:
            jobs.append(pathlib.Path(job))
        
        os.chdir(cwd)
        op_out = OPIO({
            "output" : op_in["input"],
            "njobs": njobs, 
            "jobs": jobs
        })
        return op_out

class VASPDFPT(OP):
    """
    class for VASP DFPT calculation
    """
    def __init__(self,infomode=1):
        self.infomode = infomode
    
    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'input_dfpt': Artifact(Path),
        })
    
    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'output_dfpt': Artifact(Path)                                                                                                                                         
        })
    
    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        cwd = os.getcwd()
        os.chdir(op_in["input_dfpt"])
        cmd = "bash -c \"source /opt/intel/oneapi/setvars.sh && ulimit -s unlimited && mpirun -n 64 /opt/vasp.5.4.4/bin/vasp_std \""
        subprocess.call(cmd, shell=True)
        os.chdir(cwd)
        op_out = OPIO({
            "output_dfpt": op_in["input_dfpt"]
        })
        return op_out

class PhononPostVASPDFPT(OP):
    """
    class for analyse calculation results
    """
    def __init__(self):
        pass
    
    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'input_post': Artifact(Path)
        })
    
    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'output_post': Artifact(Path)                                                                                                                                          
        })
    
    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        os.chdir(op_in["input_post"])
        cwdd = os.getcwd()
        try:
            os.chdir(os.path.join(cwdd,"work_dir"))
            shutil.copyfile("task.000000/band.conf","band.conf")
            shutil.copyfile("task.000000/param.json","param.json")
            shutil.copyfile("task.000000/POSCAR-unitcell","POSCAR-unitcell")
        except:
            pass
        parameter = loadfn("param.json")["properties"]
        inter_param_prop = loadfn("param.json")["interaction"]
        parameter['primitive'] = parameter.get('primitive', False)
        parameter['approach'] =parameter.get('approach', "linear")
        band_path = parameter['band_path']
        supercell_matrix = parameter['supercell_matrix']
        primitive = parameter['primitive']
        approach = parameter['approach']
        
        if(approach == "linear"):
            if os.path.isfile('vasprun.xml'):
                os.system('phonopy --fc vasprun.xml')
                if os.path.isfile('FORCE_CONSTANTS'):
                    os.system('phonopy --dim="%s %s %s" -c POSCAR-unitcell band.conf'%(supercell_matrix[0],supercell_matrix[1],supercell_matrix[2]))
                    os.system('phonopy-bandplot --gnuplot band.yaml > band.dat')
                    print('band.dat is created')
                    shutil.copyfile("band.dat","../band.dat")
                else:
                    print('FORCE_CONSTANTS No such file')
            else:
                print('vasprun.xml No such file')

        elif(approach == "displacement"):
            os.chdir(os.path.join(cwdd,"work_dir"))
            shutil.copyfile("task.000000/band.conf","band.conf")
            shutil.copyfile("task.000000/phonopy_disp.yaml","phonopy_disp.yaml")
            os.system('phonopy -f task.0*/vasprun.xml')
            if os.path.exists("FORCE_SETS"):
                print('FORCE_SETS is created')
            else:
                print('FORCE_SETS can not be created')
            os.system('phonopy --dim="%s %s %s" -c POSCAR-unitcell band.conf'%(supercell_matrix[0],supercell_matrix[1],supercell_matrix[2]))
            os.system('phonopy-bandplot --gnuplot band.yaml > band.dat')
            shutil.copyfile("band.dat","task.000000/band.dat")

        op_out = OPIO({
            "output_post": op_in["input_post"]
        })
        return op_out
