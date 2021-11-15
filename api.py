import time
import json
import logging
from typing import Optional
import pandas as pd
import uvicorn
from fastapi import BackgroundTasks, FastAPI
# from pydantic import BaseModel

from service.get_data import get_all_data
from service.dttot import get_similarity
import warnings
warnings.filterwarnings("ignore")

app = FastAPI()

# class UserIn(BaseModel):
#     Nama: Optional[str]=None
#     NIK: Optional[str]=None
#     DOB: Optional[str]=None
#     POB: Optional[str]=None
#     Alamat: Optional[str]=None

    # username: str
    # password: str
    # email: EmailStr
    # full_name: Optional[str] = None


# class UserOut(BaseModel):
#     "Reccomendation" : str
#     "Nama" : str
#     "Similarity_Score" : str
#     "NIK" : str
#     "DOB" : str
#     "POB" : str
#     "Alamat" : str
#     "Note" : str

    # username: str
    # email: EmailStr
    # full_name: Optional[str] = None


# @app.post("/user/", response_model=UserOut)
# async def create_user(user: UserIn):
#     return user

def get_constraint():
    file_path = "./data/Constraint_PPATK.csv"
    df = pd.read_csv(file_path)
    return df

def get_input_char(df, nama):
    input_char = ''.join([i[:4] for i in nama.strip().split(' ')])
    df = df[df["4_char"].str.contains(input_char)].reset_index(drop=True)
    return df

def DOB_similarity(df, col, dob_input):
    df = df[df[col].str.contains(dob_input)].reset_index(drop=True)
    return df

def NIK_similarity(df, col, NIK_input):
    df = df[df[col].str.contains(NIK_input)].reset_index(drop=True)
    return df

def POB_similarity(df, col, pob_input):
    try:
        df = df[df[col].str.contains(pob_input)].reset_index(drop=True)
    except:
        df = df[df[col].str.contains(pob_input).fillna(False)].reset_index(drop=True)
    return df

def nama_similarity(df, input_nama, treshold_value):
    df = get_similarity(df, input_nama, treshold_value)
    return df

def to_json(df):
    return df.to_json(orient='records')

def treatment_constraint(nama_status, nik_status, dob_status, pob_status):
    df = get_constraint()
    dict_value = {"nama" :nama_status,
                "nik" : nik_status,
                "dob" : dob_status,
                "pob" : pob_status}
    result = df.loc[(df[list(dict_value)] == pd.Series(dict_value)).all(axis=1)]
    result_recommendation =  list(set(result["recommendation"]))[0]
    return result_recommendation


@app.get('/PPATK/')
async def dttot(Nama: Optional[str]=None, NIK: Optional[str]=None, DOB: Optional[str]=None, POB: Optional[str]=None, Alamat: Optional[str]=None):
    # initialization some variable
    nama_status = "not match"
    nik_status = "not match"
    dob_status = "not match"
    pob_status = "not match"
    alamat_status = "not match"
    Similarity_Percentage = 0.8
    dict_filter = {}

    # get data
    df = get_all_data()

    start_time = time.time()
    # filter nama berdasarkan 4 character awal untuk setiap kata
    if Nama is not None:
        df_nama = get_input_char(df, Nama)
        if df_nama.shape[0] > 0:
            df = df_nama.copy()
            dict_filter["Nama"] = Nama
            nama_status = "match"
    print("--- %s seconds ---" % (time.time() - start_time))

    # filter DOB_similarity
    if DOB is not None:
        df_DOB = DOB_similarity(df, 'Tanggal Lahir', DOB)
        if df_DOB.shape[0] > 0:
            df = df_DOB.copy()
            dob_status = "match"

    # filter NIK_input
    if NIK is not None:
        df_NIK = NIK_similarity(df, 'NIK', NIK)
        if df_NIK.shape[0] > 0:
            df = df_NIK.copy()
            dict_filter["NIK"] = NIK
            if len(NIK) <= 5:
                nik_status = "not match"
            else:
                nik_status = "match"

    # filter POB_similarity
    if POB is not None:
        POB = POB.lower()
        df_POB = POB_similarity(df, 'Tempat Lahir', POB)
        if df_POB.shape[0] > 0 :
            df = df_POB.copy()
            dict_filter["Tpt Lahir"] = POB
            pob_status = "match"

    # set Note output
    statusList = [nama_status, nik_status, dob_status, pob_status, alamat_status]
    if 'match' in (statusList):
        outp = to_json(df)
    else:
        outp = None

    # get Similarity_Score
    simalarity_value = None
    if nama_status == "match":
        df = nama_similarity(df, Nama, Similarity_Percentage)
        simalarity_value = df["similarity"][0]
        if simalarity_value < 0.8:
        #     dict_filter["Nama"] = Nama
            nama_status = "not match"

    reccomendation = treatment_constraint(nama_status, nik_status, dob_status, pob_status)

    respond_out = {
        "Recommendation" : reccomendation,
        # "Nama" : nama_status,
        # "Similarity_Score" : simalarity_value,
        "Nama Similarity" : simalarity_value,
        "NIK" : nik_status,
        "DOB" : dob_status,
        "POB" : pob_status,
        "Alamat" : alamat_status,
        "Note" : outp
    }
    return respond_out


if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8090, log_level="info", reload=True)

# to run python api.py
# go here http://127.0.0.1:8090/docs
