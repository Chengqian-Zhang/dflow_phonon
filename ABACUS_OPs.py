from dflow import config, s3_config
config["host"] = "xxx"
s3_config["endpoint"] = "xxx"
config["k8s_api_server"] = "xxx"
config["token"] = "xxx"

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

import subprocess, os, shutil, glob
from pathlib import Path
from typing import List
from monty.serialization import loadfn
from dflow.plugins.bohrium import BohriumContext, BohriumExecutor
from monty.serialization import loadfn

from dflow.python import upload_packages
upload_packages.append(__file__)

class PhononMakeABACUS(OP):
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

        os.chdir(op_in["input"])
        work_d = os.getcwd()

        parameter = loadfn("param.json")["properties"]
        band_path = parameter['band_path']
        MESH = parameter.get('MESH',None)
        PRIMITIVE_AXES = parameter.get('PRIMITIVE_AXES',None)
        BAND_POINTS = parameter.get('BAND_POINTS',None)
        supercell_matrix = parameter['supercell_matrix']
        BAND_CONNECTION = parameter.get('BAND_CONNECTION',True)
        
        ## get atom type and pseu
        atom_type = []
        pseu_type = []
        with open("STRU","r") as fp:
            lines = fp.readlines()
            tag = 0
            atom_lines = []
            for line in lines:
                if(tag == 1):
                    if(line.split() == []):
                        break
                    atom_lines.append(line)
                if("ATOMIC_SPECIES") in line:
                    tag = 1
        for line in atom_lines:
            atom_type.append(line.split()[0])
            pseu_type.append(line.split()[2])
        orb_type = glob.glob("*orb")

        ## setting.conf
        ret = ""
        ret += "DIM=%s %s %s\n"%(supercell_matrix[0],supercell_matrix[1],supercell_matrix[2])
        ret += "ATOM_NAME ="
        for atom in atom_type:
            ret += " %s"%(atom)
        ret += "\n"
        with open("setting.conf","a") as fp:
            fp.write(ret)
        
        ## generate STRU-00x
        cmd = "phonopy setting.conf --abacus -d"
        subprocess.call(cmd,shell=True)

        ## band.conf
        ret = ""
        ret += "ATOM_NAME ="
        for ii in atom_type:
            ret += " %s"%(ii)
        ret += "\n"
        ret += "DIM = %s %s %s\n"%(supercell_matrix[0],supercell_matrix[1],supercell_matrix[2])
        if MESH:
            ret += "MESH = %s %s %s\n"%(MESH[0],MESH[1],MESH[2])
        if PRIMITIVE_AXES:
            ret += "PRIMITIVE_AXES = %s\n"%(PRIMITIVE_AXES)
        ret += "BAND = %s\n"%(band_path)
        if BAND_POINTS:
            ret += "BAND_POINTS = %s\n"%(BAND_POINTS)
        if BAND_CONNECTION:
            ret += "BAND_CONNECTION = .TRUE.\n"
        else :
            ret += "BAND_CONNECTION = .FALSE.\n"
        with open("band.conf","a") as fp:
            fp.write(ret)

        ## generate task.0000xx
        task_list = []
        stru_list = glob.glob("STRU-0*")
        n_task = len(stru_list)
        for ii in range(n_task):
            output_task = os.path.join(work_d,'task.%06d' % ii)
            os.makedirs(output_task,exist_ok=True)
            os.chdir(output_task)
            task_list.append(output_task)
            os.symlink(os.path.join(work_d,stru_list[ii]),'STRU')
            os.symlink(os.path.join(work_d,'STRU'),'STRU.ori')
            for pseu in pseu_type:
                os.symlink(os.path.join(work_d,pseu),pseu)
            for orb in orb_type:
                os.symlink(os.path.join(work_d,orb),orb)
            os.symlink(os.path.join(work_d,'INPUT'),'INPUT')
            os.symlink(os.path.join(work_d,'band.conf'),'band.conf')
            os.symlink(os.path.join(work_d,'param.json'),'param.json')
            os.symlink(os.path.join(work_d,'phonopy_disp.yaml'),'phonopy_disp.yaml')
            try:
                os.symlink(os.path.join(work_d,'KPT'),'KPT')
            except:
                pass
            
        ## return jobs
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
         
class ABACUS(OP):              
    """
    class for ABACUS scf calculation
    """
    def __init__(self,infomode=1):
        self.infomode = infomode
    
    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'input_abacus': Artifact(Path),
        })
    
    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'output_abacus': Artifact(Path)                                                                                                                                         
        })
    
    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        cwd = os.getcwd()
        os.chdir(op_in["input_abacus"])
        cmd = "bash -c \"source /opt/intel/oneapi/setvars.sh && ulimit -s unlimited && mpirun -n 64 abacus \""
        subprocess.call(cmd, shell=True)
        os.chdir(cwd)
        op_out = OPIO({
            "output_abacus": op_in["input_abacus"]
        })
        return op_out
        
class PhononPostABACUS(OP):
    """
    class for analyse calculation results
    """
    def __init__(self):
        pass
    
    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'input_post_abacus': Artifact(Path)
        })
    
    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'output_post_abacus': Artifact(Path)                                                                                                                                          
        })
    
    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        os.chdir(op_in["input_post_abacus"])
        cwdd = os.getcwd()
        try:
            os.chdir(os.path.join(cwdd,"work_dir"))
            shutil.copyfile("task.000000/band.conf","band.conf")
            shutil.copyfile("task.000000/param.json","param.json")
            shutil.copyfile("task.000000/STRU","STRU")
            shutil.copyfile("task.000000/phonopy_disp.yaml","phonopy_disp.yaml")
        except:
            pass
        parameter = loadfn("param.json")["properties"]

        if(os.path.exists("OUT.ABACUS")):
            os.system('cp STRU STRU.dis && cp STRU.ori STRU')
            os.system('phonopy -f OUT.ABACUS/running_scf.log')
        else:
            os.system('phonopy -f task.0*/OUT.ABACUS/running_scf.log')
        if os.path.exists("FORCE_SETS"):
            print('FORCE_SETS is created')
        else:
            print('FORCE_SETS can not be created')
        os.system('phonopy band.conf --abacus')
        os.system('phonopy-bandplot --gnuplot band.yaml > band.dat')
        try:
            shutil.copyfile("band.dat","task.000000/band.dat")
        except:
            pass

        op_out = OPIO({
            "output_post_abacus": op_in["input_post_abacus"]
        })
        return op_out
