import pandas as pd
import numpy as np
import json


def sum_by_day(timestamp,LP_Pool_State,LS_Opening,MP_Asset,pool_id,args):#Check!!
    l = LS_Opening.loc[LS_Opening["LS_timestamp"]==timestamp]
    l = l.groupby(["LP_Pool_id"]).sum()#groupby(['LP_Pool_id'])
    l = l.reset_index()
    ids = pool_id.loc[pool_id["LP_symbol"] == args["currency_stable"]]["LP_Pool_id"]
    #ids.values[0]
    #split by pools - stable = stable + all other pools, asset every pid != stable
    stable = l.loc[l["LP_Pool_id"]== ids.values[0]]
    stable.reset_index()
    asset = l.loc[l["LP_Pool_id"]!= ids.values[0]]
    asset = asset[["LS_timestamp","LS_loan_amnt_stable","LP_Pool_id"]]

    asset = pd.merge(asset,pool_id,on="LP_Pool_id", how='left')
    asset = asset.rename(columns={"LP_symbol":"symbol"})
    symbol_digit = pd.DataFrame(args["symbol_digit"])
    asset = pd.merge(asset,symbol_digit, on="symbol", how="left")
    m=MP_Asset
    m = m.rename(columns={"MP_asset_symbol":"symbol","MP_timestamp":"LS_timestamp"})

    asset = pd.merge(asset,m,on=["LS_timestamp","symbol"],how="left")
    asset["LS_loan_amnt_stable"] = asset["LS_loan_amnt_stable"]/(asset["MP_price_in_stable"]/10**asset["digit"])
    asset["LS_loan_amnt_stable"] = asset["LS_loan_amnt_stable"].round(0).astype("uint64")
    asset = asset.drop(["symbol","digit","MP_price_in_stable"], axis=1)
    #print(asset,stable)
    asset = asset.rename(columns={"LS_timestamp":"LP_Pool_timestamp","LS_loan_amnt_stable":"LP_Pool_total_borrowed_asset"})
    stable = stable[["LS_timestamp","LS_loan_amnt_stable","LP_Pool_id"]]
    stable = stable.rename(columns={"LS_timestamp":"LP_Pool_timestamp","LS_loan_amnt_stable":"LP_Pool_total_borrowed_stable"})

    LP_Pool_State = pd.merge(LP_Pool_State,stable,on=["LP_Pool_timestamp","LP_Pool_id"],how="left").fillna(0)
    LP_Pool_State = pd.merge(LP_Pool_State,asset,on=["LP_Pool_timestamp","LP_Pool_id"],how="left").fillna(0)
    return LP_Pool_State

def lp_tdep(LP_Pool_State, LP_Deposit, pool_id, args):
    l = LP_Deposit.groupby(["LP_timestamp", "LP_Pool_id"]).sum()
    l = l.reset_index()
    ids = pool_id.loc[pool_id["LP_symbol"] == args["currency_stable"]]["LP_Pool_id"]
    asset = l.loc[l["LP_Pool_id"] != ids.values[0]]
    asset.reset_index()
    asset = asset[["LP_timestamp", "LP_amnt_asset", "LP_Pool_id"]]
    stable = l[["LP_timestamp", "LP_amnt_stable", "LP_Pool_id", ]]
    new = pd.merge(stable, asset, on=["LP_timestamp", "LP_Pool_id"], how="left")
    new = new.rename(
        columns={"LP_timestamp": "LP_Pool_timestamp", "LP_amnt_stable": "LP_Pool_total_deposited_stable",
                 "LP_amnt_asset": "LP_Pool_total_deposited_asset"})
    LP_Pool_State = pd.merge(LP_Pool_State, new, on=["LP_Pool_timestamp", "LP_Pool_id"], how='left')
    return LP_Pool_State


def LP_Pool_State_gen():
    LP_Pool_State = pd.DataFrame({"LP_Pool_id":[],"LP_Pool_timestamp":[],"LP_Pool_total_value_locked_stable":[],"LP_Pool_total_value_locked_asset":[],"LP_Pool_total_issued_receipts":[],"LP_Pool_total_borrowed_stable":[],"LP_Pool_total_borrowed_asset":[],"LP_Pool_total_yield_stable":[],
                                  "LP_Pool_total_yield_asset":[],"SYS_LP_Pool_TV_IntDep_stable":[],"LP_Pool_total_deposited_stable":[],"LP_Pool_total_deposited_asset":[],"SYS_LS_interest":[],"SYS_LS_Pool_interest":[],"SYS_TR_interest":[]})
    return LP_Pool_State

def lpps_create_record(timestamp,LS_Opening,LP_Deposit,MP_Asset,LP_Pool,args):

    pool_id = LP_Pool
    lpps = pd.DataFrame()
    tmp = pd.DataFrame({"LP_Pool_timestamp": np.repeat(timestamp,len(pool_id)), "LP_Pool_id": pool_id["LP_Pool_id"]})
    lpps = pd.concat([lpps, tmp], axis=0, ignore_index=True)

    lpps = sum_by_day(timestamp, lpps, LS_Opening, MP_Asset, pool_id, args)
    return lpps
    LP_Pool_State["LP_Pool_total_borrowed_stable"] = LP_Pool_State["LP_Pool_total_borrowed_stable"] + LP_Pool_State[
        "LP_Pool_total_borrowed_asset"]
    LP_Pool_State = lp_tdep(LP_Pool_State, LP_Deposit, pool_id, args).fillna(0)
    ids = pool_id.loc[pool_id["LP_symbol"] == args["currency_stable"]]["LP_Pool_id"]

    a = pd.DataFrame(args["symbol_digit"])
    c = a.loc[a["symbol"] == args["currency_stable"]]["digit"].values[0].astype(float)
    LP_Pool_State.loc[LP_Pool_State["LP_Pool_timestamp"] == min(LP_Pool_State["LP_Pool_timestamp"]), [
        "LP_Pool_total_deposited_stable"]] = args["startup_pool_value"] * 10 ** c
    LP_Pool_State["SYS_LS_interest"] = 0
    LP_Pool_State["SYS_LS_Pool_interest"] = 0
    LP_Pool_State["SYS_TR_interest"] = 0
    # LP_total_deposited is not yet deletet - probably will be removed in next version
    LP_Pool_State["SYS_LP_Pool_TV_IntDep_stable"] = LP_Pool_State["LP_Pool_total_deposited_stable"]

    return  lpps

def LP_Pool_State_update(timestamp,lpps_prev,LP_Pool_State,LS_Opening, LP_Deposit,MP_Asset, LP_Pool,flag, args):
    #generate lp pool state record for the current day if flag is up LP_Pool_State = lpps_prev
    if flag:
        LP_Pool_State = LP_Pool_State.drop(LP_Pool_State.loc[LP_Pool_State["LP_Pool_timestamp"]==timestamp].index,axis=0)
        LP_Pool_State = pd.concat([LP_Pool_State,lpps_prev],axis=0)
        flag = False
        return LP_Pool_State,flag
    else:
        lpps = lpps_create_record(timestamp,LS_Opening,LP_Deposit,MP_Asset,LP_Pool,args)
        LP_Pool_State = pd.concat([LP_Pool_State,lpps],axis=0)
        return LP_Pool_State,flag
