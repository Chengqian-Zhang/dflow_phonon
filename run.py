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
from VASP_OPs import PhononMakeVASP,VASPDFPT,PhononPostVASPDFPT

def main_vasp():
    cwd = os.getcwd()
    work_dir = os.path.join(cwd,"work_dir")
    wf = Workflow(name = "phonon",context=brm_context, host="https://workflow.dp.tech/")

    phononmake = Step(
        name="Phononmake", 
        template=PythonOPTemplate(PhononMakeVASP,image="registry.dp.tech/dptech/prod-11461/phonopy:v1",command=["python3"]),
        artifacts={"input":upload_artifact(work_dir)},
        )
    wf.add(phononmake)
   
    vasp = PythonOPTemplate(VASPDFPT,slices=Slices("{{item}}", input_artifact=["input_dfpt"],output_artifact=["output_dfpt"]),image='registry.dp.tech/dptech/vasp:5.4.4-dflow',command=["python3"])
    #njobs = phononmake.outputs.parameters["njobs"] 
    vasp_cal = Step("VASP-Cal",template=vasp,artifacts={"input_dfpt":phononmake.outputs.artifacts["jobs"]},with_param=argo_range(phononmake.outputs.parameters["njobs"]),key="VASP-Cal-{{item}}",util_image='registry.dp.tech/dptech/vasp:5.4.4-dflow',util_command=['python3'],executor=BohriumExecutor(executor="bohrium_v2", extra={"scassType":"c16_m32_cpu","projectId": 10080,"jobType":"container", "logFiles": []}))   
    wf.add(vasp_cal)

    phononpost = Step(
        name="Phononpost", 
        template=PythonOPTemplate(PhononPostVASPDFPT,image="registry.dp.tech/dptech/prod-11461/phonopy:v1",command=["python3"]),
        artifacts={"input_post":vasp_cal.outputs.artifacts["output_dfpt"]},
        )
    wf.add(phononpost)

    wf.submit()

    while wf.query_status() in ["Pending","Running"]:
        time.sleep(4)
    assert(wf.query_status() == 'Succeeded')
    #step0 = wf.query_step(name="Phononmake")[0]
    #download_artifact(step0.outputs.artifacts["output"])
    #step1 = wf.query_step(name="VASP-Cal")[0]
    #download_artifact(step1.outputs.artifacts["output_dfpt"])
    step2 = wf.query_step(name="Phononpost")[0]
    download_artifact(step2.outputs.artifacts["output_post"])

    ##本地后处理
    os.chdir("work_dir")
    shutil.copyfile("task.000000/band.dat","result.out")

def main_dp():
    cwd = os.getcwd()
    work_dir = os.path.join(cwd,"work_dir")
    wf = Workflow(name = "phonon",context=brm_context, host="https://workflow.dp.tech/")

    phononmake = Step(
        name="Phononmake",
        template=PythonOPTemplate(PhononMakeDP,image="registry.dp.tech/dptech/prod-11461/phonopy:v1",command=["python3"]),
        artifacts={"input":upload_artifact(work_dir)},
        )
    wf.add(phononmake)
    ''' 
    dp = PythonOPTemplate(DP,image='registry.dp.tech/dptech/prod-11461/phonolammps:v1',command=["python3"])
    dp_cal = Step("DP-Cal",template=dp,artifacts={"input_dp":phononmake.outputs.artifacts["output"]},executor=BohriumExecutor(executor="bohrium_v2", extra={"scassType":"c4_m15_1 * NVIDIA T4","projectId": 10080,"jobType":"container", "logFiles": []}))
    wf.add(dp_cal)
    '''
    dp = PythonOPTemplate(DP,image='registry.dp.tech/dptech/prod-11461/phonolammps:v1',command=["python3"])
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
    #step0 = wf.query_step(name="Phononmake")[0]
    #download_artifact(step0.outputs.artifacts["output"])
    #step1 = wf.query_step(name="VASP-Cal")[0]
    #download_artifact(step1.outputs.artifacts["output_dfpt"])
    step2 = wf.query_step(name="Phononpost")[0]
    download_artifact(step2.outputs.artifacts["output_post_dp"])

if __name__ == "__main__":
    brm_context = BohriumContext(
        executor="mixed",
        #executor="bohrium_v2",
        extra={"scass_type":"c16_m32_cpu","program_id":10080,"job_type":"container"}, # 全局bohrium配置
        username="xxx",
        password="xxx"
    ) 

    parameter = loadfn(os.path.join("work_dir","param.json"))["properties"]
    inter_param_prop = loadfn(os.path.join("work_dir","param.json"))["interaction"]
    cal_type = inter_param_prop["type"]
    if(cal_type == "vasp"):
        main_vasp()
    elif(cal_type == "dp"):
        main_dp()
