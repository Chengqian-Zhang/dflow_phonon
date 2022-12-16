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
from DP_OPs import element_list,PhononMakeDP,DP,PhononPostDP
from VASP_OPs import PhononMakeVASP,VASP,PhononPostVASP
from ABACUS_OPs import PhononMakeABACUS,ABACUS,PhononPostABACUS

def main_vasp():
    global_param = loadfn("global.json")
    work_dir = global_param.get("work_dir",None)
    email = global_param.get("email",None)
    password = global_param.get("password",None)
    program_id = global_param.get("program_id",None)
    phonopy_image_name = global_param.get("phonopy_image_name",None)
    vasp_image_name = global_param.get("vasp_image_name",None)
    cpu_scass_type = global_param.get("cpu_scass_type",None)
    gpu_scass_type = global_param.get("gpu_scass_type",None)

    cwd = os.getcwd()
    work_dir = os.path.join(cwd,work_dir)
    wf = Workflow(name = "phonon",context=brm_context, host="https://workflow.dp.tech/")

    phononmake = Step(
        name="Phononmake", 
        template=PythonOPTemplate(PhononMakeVASP,image="registry.dp.tech/dptech/prod-11461/phonopy:v1",command=["python3"]),
        artifacts={"input":upload_artifact(work_dir)},
        )
    wf.add(phononmake)
   
    vasp = PythonOPTemplate(VASP,slices=Slices("{{item}}", input_artifact=["input_dfpt"],output_artifact=["output_dfpt"]),image=vasp_image_name,command=["python3"])
    vasp_cal = Step("VASP-Cal",template=vasp,artifacts={"input_dfpt":phononmake.outputs.artifacts["jobs"]},with_param=argo_range(phononmake.outputs.parameters["njobs"]),key="VASP-Cal-{{item}}",util_image=vasp_image_name,util_command=['python3'],executor=BohriumExecutor(executor="bohrium_v2", extra={"scassType":cpu_scass_type,"projectId": program_id,"jobType":"container", "logFiles": []}))   
    wf.add(vasp_cal)

    phononpost = Step(
        name="Phononpost", 
        template=PythonOPTemplate(PhononPostVASP,image="registry.dp.tech/dptech/prod-11461/phonopy:v1",command=["python3"]),
        artifacts={"input_post":vasp_cal.outputs.artifacts["output_dfpt"]},
        )
    wf.add(phononpost)

    wf.submit()

    while wf.query_status() in ["Pending","Running"]:
        time.sleep(4)
    assert(wf.query_status() == 'Succeeded')
    step2 = wf.query_step(name="Phononpost")[0]
    download_artifact(step2.outputs.artifacts["output_post"])

def main_abacus():
    global_param = loadfn("global.json")
    work_dir = global_param.get("work_dir",None)
    email = global_param.get("email",None)
    password = global_param.get("password",None)
    program_id = global_param.get("program_id",None)
    phonopy_image_name = global_param.get("phonopy_image_name",None)
    abacus_image_name = global_param.get("abacus_image_name",None)
    cpu_scass_type = global_param.get("cpu_scass_type",None)
    gpu_scass_type = global_param.get("gpu_scass_type",None)

    cwd = os.getcwd()
    work_dir = os.path.join(cwd,work_dir)
    wf = Workflow(name = "phonon",context=brm_context, host="https://workflow.dp.tech/")

    phononmake = Step(
        name="Phononmake", 
        template=PythonOPTemplate(PhononMakeABACUS,image=phonopy_image_name,command=["python3"]),
        artifacts={"input":upload_artifact(work_dir)},
        )
    wf.add(phononmake)
    '''
    abacus = PythonOPTemplate(ABACUS,slices=Slices("{{item}}", input_artifact=["input_abacus"],output_artifact=["output_abacus"]),image=abacus_image_name,command=["python3"])
    abacus_cal = Step("ABACUS-Cal",template=abacus,artifacts={"input_abacus":phononmake.outputs.artifacts["jobs"]},with_param=argo_range(phononmake.outputs.parameters["njobs"]),key="ABACUS-Cal-{{item}}",util_image=abacus_image_name,util_command=['python3'])  
    wf.add(abacus_cal)
    '''
    
    abacus = PythonOPTemplate(ABACUS,slices=Slices("{{item}}", input_artifact=["input_abacus"],output_artifact=["output_abacus"]),image=abacus_image_name,command=["python3"])
    abacus_cal = Step("ABACUS-Cal",template=abacus,artifacts={"input_abacus":phononmake.outputs.artifacts["jobs"]},with_param=argo_range(phononmake.outputs.parameters["njobs"]),key="ABACUS-Cal-{{item}}",util_image=abacus_image_name,util_command=['python3'],executor=BohriumExecutor(executor="bohrium_v2", extra={"scassType":cpu_scass_type,"projectId": program_id,"jobType":"container", "logFiles": []}))   
    wf.add(abacus_cal)
    
    phononpost = Step(
        name="Phononpost", 
        template=PythonOPTemplate(PhononPostABACUS,image=phonopy_image_name,command=["python3"]),
        artifacts={"input_post_abacus":abacus_cal.outputs.artifacts["output_abacus"]},
        )
    wf.add(phononpost)
    
    wf.submit()

    while wf.query_status() in ["Pending","Running"]:
        time.sleep(4)
    assert(wf.query_status() == 'Succeeded')
    '''
    step2 = wf.query_step(name="ABACUS-Cal")[0]
    download_artifact(step2.outputs.artifacts["output_abacus"])
    '''
    step2 = wf.query_step(name="Phononpost")[0]
    download_artifact(step2.outputs.artifacts["output_post_abacus"])

def main_dp():
    global_param = loadfn("global.json")
    work_dir = global_param.get("work_dir",None)
    email = global_param.get("email",None)
    password = global_param.get("password",None)
    program_id = global_param.get("program_id",None)
    phonopy_image_name = global_param.get("phonopy_image_name",None)
    phonolammps_image_name = global_param.get("phonolammps_image_name",None)
    cpu_scass_type = global_param.get("cpu_scass_type",None)
    gpu_scass_type = global_param.get("gpu_scass_type",None)

    cwd = os.getcwd()
    work_dir = os.path.join(cwd,work_dir)
    wf = Workflow(name = "phonon",context=brm_context, host="https://workflow.dp.tech/")

    phononmake = Step(
        name="Phononmake",
        template=PythonOPTemplate(PhononMakeDP,image="registry.dp.tech/dptech/prod-11461/phonopy:v1",command=["python3"]),
        artifacts={"input":upload_artifact(work_dir)},
        )
    wf.add(phononmake)
    '''
    dp = PythonOPTemplate(DP,image=phonolammps_image_name,command=["python3"])
    dp_cal = Step("DP-Cal",template=dp,artifacts={"input_dp":phononmake.outputs.artifacts["output"]},executor=BohriumExecutor(executor="bohrium_v2", extra={"scassType":gpu_scass_type,"projectId": program_id,"jobType":"container", "logFiles": []}))
    wf.add(dp_cal)
    '''
    dp = PythonOPTemplate(DP,image=phonolammps_image_name,command=["python3"])
    dp_cal = Step("DP-Cal",template=dp,artifacts={"input_dp":phononmake.outputs.artifacts["output"]})
    wf.add(dp_cal)

    phononpost = Step(
        name="Phononpost",
        template=PythonOPTemplate(PhononPostDP,image="registry.dp.tech/dptech/prod-11461/phonopy:v1",command=["python3"]),
        artifacts={"input_post_dp":dp_cal.outputs.artifacts["output_dp"]},
        )
    wf.add(phononpost)

    wf.submit()

    while wf.query_status() in ["Pending","Running"]:
        time.sleep(4)
    assert(wf.query_status() == 'Succeeded')
    step2 = wf.query_step(name="Phononpost")[0]
    download_artifact(step2.outputs.artifacts["output_post_dp"])

if __name__ == "__main__":
    global_param = loadfn("global.json")
    work_dir = global_param.get("work_dir",None)
    email = global_param.get("email",None)
    password = global_param.get("password",None)
    program_id = global_param.get("program_id",None)
    cpu_scass_type = global_param.get("cpu_scass_type",None)
    gpu_scass_type = global_param.get("gpu_scass_type",None)
    brm_context = BohriumContext(
        executor="mixed",
        #executor="bohrium_v2",
        extra={"scass_type":cpu_scass_type,"program_id":program_id,"job_type":"container"}, # 全局bohrium配置
        username=email,
        password=password
    ) 
    
    inter_param_prop = loadfn(os.path.join(work_dir,"param.json"))["interaction"]
    cal_type = inter_param_prop["type"]
    if(cal_type == "vasp"):
        main_vasp()
    elif(cal_type == "dp"):
        main_dp()
    elif(cal_type == "abacus"):
        main_abacus()
