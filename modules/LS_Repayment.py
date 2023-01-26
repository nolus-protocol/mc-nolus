import json
import pandas as pd
import numpy as np
import random
import modules.gnrl as gnrl
import modules.LP_Pool_State as lpps
import datetime


def lsr_timestamp(LS_Opening, args):
    month = 2592000
    duration = LS_Opening["SYS_LS_expected_payment"] + LS_Opening["SYS_LS_expected_penalty"]
    timestamp = pd.DataFrame(
        {"LS_timestamp": LS_Opening["LS_timestamp"], "LS_contract_id": LS_Opening["LS_contract_id"]})
    timestamp["LS_timestamp"] = LS_Opening["LS_timestamp"].astype("datetime64").apply(
        lambda x: datetime.datetime.timestamp(x)).astype("uint64")
    timestamp["LP_Pool_id"] = LS_Opening["LP_Pool_id"]

    timestamp = pd.DataFrame(np.repeat(timestamp.values, duration, axis=0))
    timestamp = pd.DataFrame({"LS_timestamp": timestamp[0], "LS_contract_id": timestamp[1], "LP_Pool_id": timestamp[2]})
    timestamp["LS_timestamp"] = timestamp["LS_timestamp"].values.astype("uint64")
    # duration = pd.DataFrame({"duration":duration})
    d = []
    l1 = []
    for i in duration:
        l1 = [s for s in range(1, i + 1)]
        d.extend(l1)
    timestamp["Duration"] = d
    timestamp["Duration"] = timestamp["Duration"] * month
    timestamp["LS_timestamp"] = timestamp["LS_timestamp"] + timestamp["Duration"]
    timestamp["LS_timestamp"] = timestamp["LS_timestamp"].apply(datetime.date.fromtimestamp)
    timestamp = timestamp.drop("Duration", axis=1)

    return timestamp


def LS_Repayment_generate(LS_Opening, pool_id,
                          args):  # WRONG  witthout penalty add column flag if 1  =  payment if 2 penalty

    LS_Repayment = pd.DataFrame(lsr_timestamp(LS_Opening, args))
    lp_dep_h = [s for s in range(100, 100 + len(LS_Repayment))]
    lp_dep_idx = lp_dep_h
    LS_Repayment["LS_repayment_height"] = lp_dep_h
    LS_Repayment["LS_repayment_idx"] = lp_dep_idx
    LS_Repayment = pd.merge(LS_Repayment, pool_id, on="LP_Pool_id", how='left')
    LS_Repayment = LS_Repayment.rename(columns={"LP_symbol": "LS_symbol"})
    LS_Repayment = LS_Repayment.drop("LP_Pool_id", axis=1)
    LS_Repayment["LS_amnt_stable"] = 0
    LS_Repayment["LS_principal_stable"] = 0
    LS_Repayment["LS_current_margin_stable"] = 0
    LS_Repayment["LS_current_interest_stable"] = 0
    LS_Repayment["LS_prev_interest_stable"] = 0
    LS_Repayment["LS_prev_margin_stable"] = 0

    return LS_Repayment