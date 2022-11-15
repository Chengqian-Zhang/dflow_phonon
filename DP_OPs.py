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

def element_list(type_map):
    type_map_reverse = {k: v for v, k in type_map.items()}
    type_map_list = []
    tmp_list = list(type_map_reverse.keys())
    tmp_list.sort()
    for ii in tmp_list:
        type_map_list.append(type_map_reverse[ii])
    return type_map_list

class PhononMakeDP(OP):
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
        type_map = inter_param_prop["type_map"]
        type_map_list = element_list(type_map)

        if primitive:
            subprocess.call('phonopy --symmetry',shell=True)
            subprocess.call('cp PPOSCAR POSCAR',shell=True)
            shutil.copyfile("PPOSCAR","POSCAR-unitcell")
        else:
            shutil.copyfile("POSCAR","POSCAR-unitcell")
        
        with open("POSCAR","r") as fp:
            lines = fp.read().split('\n')
            ele_list = lines[5].split()
        
        output_task = os.path.join(work_d,'task.000000')
        os.makedirs(output_task,exist_ok=True)
        os.chdir(output_task)
        for jj in ['INCAR', 'POTCAR', 'POSCAR', 'conf.lmp', 'in.lammps','POSCAR-unitcell','SPOSCAR']:
            if os.path.exists(jj):
                os.remove(jj)
        os.symlink(os.path.join(work_d,"POSCAR-unitcell"),"POSCAR")
        os.symlink(os.path.join(work_d,"frozen_model.pb"),"frozen_model.pb")
        with open("POSCAR",'r') as fp :
            lines = fp.read().split('\n')
            ele_list = lines[5].split()
        with open('band.conf','w') as fp:
            fp.write('ATOM_NAME = ')
            for ii in ele_list:
                fp.write(ii)
                fp.write(' ')
            fp.write('\n')
            fp.write('DIM = %s %s %s\n'%(supercell_matrix[0],supercell_matrix[1],supercell_matrix[2]))
            fp.write('BAND = %s\n'%band_path)    
            fp.write('FORCE_CONSTANTS=READ\n')
        
        ##in.lammps
        ret = ""
        ret += "clear\n"
        ret += "units   metal\n"
        ret += "dimension       3\n"
        ret += "boundary        p p p\n"
        ret += "atom_style      atomic\n"
        ret += "box         tilt large\n"
        ret += "read_data   conf.lmp\n"
        ntypes = len(type_map_list)
        for ii in range(ntypes):
            ret += "mass            %d %f\n" % (ii + 1, Element(type_map_list[ii]).mass)
        ret += "neigh_modify    every 1 delay 0 check no\n"
        ret += "pair_style      deepmd frozen_model.pb\n"
        ret += "pair_coeff      * *\n"
        with open("in.lammps","a") as f:
            f.write(ret)
        
        #conf.lmp
        ls = dpdata.System("POSCAR")
        ls.to(fmt="lmp",file_name="conf.lmp") 
        with open("conf.lmp","r") as f:
            lines = f.readlines()
            for line in lines:
                if("types" in line):
                    wrong = int(line.split()[0])
        os.system("sed -i s/'%d atom types'/'%d atom types'/g conf.lmp"%(wrong,ntypes))

        op_out = OPIO({
            "output" : op_in["input"],
        })
        return op_out

class DP(OP):
    """
    class for VASP DFPT calculation
    """
    def __init__(self,infomode=1):
        self.infomode = infomode
    
    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'input_dp': Artifact(Path),
        })
    
    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'output_dp': Artifact(Path)                                                                                                                                         
        })
    
    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        cwd = os.getcwd()
        os.chdir(op_in["input_dp"])
        parameter = loadfn("param.json")["properties"]
        supercell_matrix = parameter['supercell_matrix']

        os.chdir(os.path.join(op_in["input_dp"],"task.000000"))
        for i in range(100):
            print(os.listdir())
        cmd = "phonolammps in.lammps -c POSCAR --dim %s %s %s "%(supercell_matrix[0],supercell_matrix[1],supercell_matrix[2])
        subprocess.call(cmd, shell=True)
        os.chdir(cwd)
        op_out = OPIO({
            "output_dp": op_in["input_dp"]
        })
        return op_out

class PhononPostDP(OP):
    """
    class for analyse calculation results
    """
    def __init__(self):
        pass
    
    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'input_post_dp': Artifact(Path)
        })
    
    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'output_post_dp': Artifact(Path)                                                                                                                                          
        })
    
    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        os.chdir(op_in["input_post_dp"])
        os.chdir("task.000000")

        parameter = loadfn("../param.json")["properties"]
        supercell_matrix = parameter['supercell_matrix']
        
        if os.path.isfile('FORCE_CONSTANTS'):
            os.system('phonopy --dim="%s %s %s" -c POSCAR band.conf'%(supercell_matrix[0],supercell_matrix[1],supercell_matrix[2]))
            os.system('phonopy-bandplot --gnuplot band.yaml > band.dat')
        else:
            print('FORCE_CONSTANTS No such file')

        op_out = OPIO({
            "output_post_dp": op_in["input_post_dp"]
        })
        return op_out
