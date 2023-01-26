import pandas as pd
import numpy as np
import datetime


def lpw_timestamp(LP_Deposit, basis, args):
    day = 86400
    month = 30 * day
    if basis == "month":
        duration = LP_Deposit["SYS_LP_expected_duration"].values
        timestamp = pd.DataFrame(
            {"LP_timestamp": LP_Deposit["LP_timestamp"], "LP_address_id": LP_Deposit["LP_address_id"],
             "SYS_LP_contract_id": LP_Deposit["SYS_LP_contract_id"]})
        timestamp["LP_timestamp"] = LP_Deposit["LP_timestamp"].astype("datetime64").apply(
            lambda x: datetime.datetime.timestamp(x)).astype(int)
        timestamp["LP_Pool_id"] = LP_Deposit["LP_Pool_id"]
        # print(type(timestamp["LP_"][0]))
        # timestamp = np.repeat(timestamp.values, duration+1, axis=0)
        # timestamp = pd.DataFrame(timestamp.values)
        timestamp = pd.DataFrame(np.repeat(timestamp.values, duration, axis=0))
        timestamp = pd.DataFrame(
            {"LP_timestamp": timestamp[0], "LP_address_id": timestamp[1], "SYS_LP_contract_id": timestamp[2],
             "LP_Pool_id": timestamp[3]})
        timestamp["LP_timestamp"] = timestamp["LP_timestamp"].values.astype("uint64")
        # duration = pd.DataFrame({"duration":duration})
        d = []
        l1 = []
        for i in duration:
            l1 = [s for s in range(1, i + 1)]
            d.extend(l1)
        timestamp["Duration"] = d
        timestamp["Duration"] = timestamp["Duration"] * month
        timestamp["LP_timestamp"] = timestamp["LP_timestamp"] + timestamp["Duration"]
        timestamp["LP_timestamp"] = timestamp["LP_timestamp"].apply(datetime.date.fromtimestamp)
        timestamp = timestamp.drop("Duration", axis=1)
    if basis == "day":
        duration = LP_Deposit["SYS_LP_expected_duration"].values
        duration = duration * 30
        #print(len(duration))
        timestamp = pd.DataFrame(
            {"LP_timestamp": LP_Deposit["LP_timestamp"], "LP_address_id": LP_Deposit["LP_address_id"],
             "SYS_LP_contract_id": LP_Deposit["SYS_LP_contract_id"]})
        timestamp["LP_timestamp"] = LP_Deposit["LP_timestamp"].astype("datetime64").apply(
            lambda x: datetime.datetime.timestamp(x)).astype(int)
        timestamp["LP_Pool_id"] = LP_Deposit["LP_Pool_id"]
        # print(type(timestamp["LP_"][0]))
        # timestamp = np.repeat(timestamp.values, duration+1, axis=0)
        # timestamp = pd.DataFrame(timestamp.values)
        timestamp = pd.DataFrame(np.repeat(timestamp.values, duration, axis=0))
        timestamp = pd.DataFrame(
            {"LP_timestamp": timestamp[0], "LP_address_id": timestamp[1], "SYS_LP_contract_id": timestamp[2],
             "LP_Pool_id": timestamp[3]})
        timestamp["LP_timestamp"] = timestamp["LP_timestamp"].values.astype("uint64")
        # duration = pd.DataFrame({"duration":duration})
        d = []
        l1 = []
        for i in duration:
            l1 = [s for s in range(1, i + 1)]
            d.extend(l1)
        timestamp["Duration"] = d
        timestamp["Duration"] = timestamp["Duration"] * day
        timestamp["LP_timestamp"] = timestamp["LP_timestamp"] + timestamp["Duration"]
        timestamp["LP_timestamp"] = timestamp["LP_timestamp"].apply(datetime.date.fromtimestamp)
        timestamp = timestamp.drop("Duration", axis=1)

    return timestamp


def LP_Withdraw_generate(LP_Deposit, pool_id, args):
    LP_Withdraw = pd.DataFrame(lpw_timestamp(LP_Deposit, "month", args))
    lp_dep_h = [s for s in range(100, 100 + len(LP_Withdraw))]
    lp_dep_idx = lp_dep_h
    LP_Withdraw["LP_withdraw_height"] = lp_dep_h
    LP_Withdraw["LP_withdraw_idx"] = lp_dep_idx
    # withdraw = Deposit*expected_duration, last means it gets closed in the end of the duration
    # generated rows could be later used for change in the ammount during the expected_duration
    SYS_LP_Withdraw = pd.DataFrame(data= None, columns=LP_Withdraw.columns)

    LP_Withdraw = LP_Withdraw.drop_duplicates(subset=["LP_address_id"], keep="last", ignore_index=True)
    LP_Withdraw["LP_deposit_close"] = True
    SYS_LP_Withdraw[["LP_amnt_stable", "LP_interest", "LP_interest_amnt"]] = 0
    LP_Withdraw["LP_amnt_stable"] = 0
    LP_Deposit["SYS_end_date"] = LP_Withdraw["LP_timestamp"]
    return LP_Deposit,LP_Withdraw, SYS_LP_Withdraw