import pandas as pd
import numpy as np
import json




def LP_Pool_State_gen(LP_Pool,min_timestamp,args):
    a = pd.DataFrame(args["symbol_digit"])
    c = a.loc[a["symbol"] == args["currency_stable"]]["digit"].values[0].astype(float)
    LP_Pool_State = pd.DataFrame({"LP_Pool_id": LP_Pool["LP_Pool_id"], "LP_Pool_timestamp": np.repeat(min_timestamp, len(LP_Pool)),
                         "LP_Pool_total_value_locked_stable": np.repeat(0, len(LP_Pool)),
                         "LP_Pool_total_value_locked_asset": np.repeat(0, len(LP_Pool)),
                         "LP_Pool_total_issued_receipts": np.repeat(0, len(LP_Pool)),
                         "LP_Pool_total_borrowed_stable": np.repeat(0, len(LP_Pool)),
                         "LP_Pool_total_borrowed_asset": np.repeat(0, len(LP_Pool)),
                         "LP_Pool_total_yield_stable": np.repeat(0, len(LP_Pool)),
                         "LP_Pool_total_yield_asset": np.repeat(0, len(LP_Pool)),
                         "SYS_LP_Pool_TV_IntDep_stable": np.repeat((args["startup_pool_value"] * 10 ** c), len(LP_Pool)),
                         "LP_Pool_total_deposited_stable": np.repeat((args["startup_pool_value"] * 10 ** c), len(LP_Pool)),
                         "LP_Pool_total_deposited_asset": np.repeat(0, len(LP_Pool)),
                         "SYS_LS_interest": np.repeat(0, len(LP_Pool)),
                         "SYS_LS_Pool_interest": np.repeat(0, len(LP_Pool)),
                         "SYS_TR_interest": np.repeat(0, len(LP_Pool))})

    return LP_Pool_State


def get_borrowed(timestamp,m, LS_Opening, LP_Pool, args):
    #borrowed_all - returns all borrowed in stable by pool
    #borrowed_asset - returns all borrowed  in pool currency
    borrowed_all = LS_Opening.loc[LS_Opening["LS_timestamp"]==timestamp]
    borrowed_all = borrowed_all[["LS_loan_amnt_stable","LP_Pool_id"]].groupby("LP_Pool_id").sum().reset_index()
    stable_id = LP_Pool.loc[LP_Pool["LP_symbol"]==args["currency_stable"],"LP_Pool_id"]
    borrowed_asset = borrowed_all.copy()
    borrowed_asset.loc[borrowed_asset["LP_Pool_id"]==stable_id[0],"LS_loan_amnt_stable"] = 0
    borrowed_all = pd.merge(borrowed_all,LP_Pool,on="LP_Pool_id",how='left')
    borrowed_all["MP_price_in_stable"] = borrowed_all["LP_Pool_id"].map(dict(m[["LP_symbol","MP_price_in_stable"]].values))
    borrowed_all["LS_loan_amnt_stable"] = borrowed_all["LS_loan_amnt_stable"]*borrowed_all["MP_price_in_stable"]
    borrowed_all = borrowed_all.drop("MP_price_in_stable",axis=1)
    return borrowed_all,borrowed_asset


def get_deposited(timestamp,m, LP_Deposit, LP_Pool, args):
    deposited_all = LP_Deposit.loc[LP_Deposit["LP_timestamp"]==timestamp]
    deposited_all = deposited_all[["LP_amnt_stable","LP_Pool_id"]].groupby("LP_Pool_id").sum().reset_index()
    stable_id = LP_Pool.loc[LP_Pool["LP_symbol"]==args["currency_stable"],"LP_Pool_id"]
    deposited_asset = deposited_all.copy()
    deposited_asset.loc[deposited_asset["LP_Pool_id"]==stable_id[0],"LP_amnt_stable"] = 0
    deposited_all["MP_price_in_stable"] = deposited_all["LP_Pool_id"].map(dict(m[["LP_symbol","MP_price_in_stable"]].values))
    deposited_all["LP_amnt_stable"] = deposited_all["LP_amnt_stable"]*deposited_all["MP_price_in_stable"]
    deposited_all = deposited_all.drop("MP_price_in_stable",axis=1)

    return deposited_all,deposited_asset


def sum_tables(lpps, lpps_prev):
    lpps = lpps.sort_values(["LP_Pool_id"])
    lpps_prev = lpps_prev.sort_values(["LP_Pool_id"])
    lpps["LP_Pool_total_value_locked_stable"] = lpps["LP_Pool_total_value_locked_stable"] + lpps_prev["LP_Pool_total_value_locked_stable"]
    lpps["LP_Pool_total_value_locked_asset"] = lpps["LP_Pool_total_value_locked_asset"] + lpps_prev["LP_Pool_total_value_locked_asset"]
    lpps["LP_Pool_total_issued_receipts"] = lpps["LP_Pool_total_issued_receipts"] + lpps_prev["LP_Pool_total_issued_receipts"]
    lpps["LP_Pool_total_borrowed_stable"] = lpps["LP_Pool_total_borrowed_stable"] + lpps_prev["LP_Pool_total_borrowed_stable"]
    lpps["LP_Pool_total_borrowed_asset"] = lpps["LP_Pool_total_borrowed_asset"] + lpps_prev["LP_Pool_total_borrowed_asset"]
    lpps["LP_Pool_total_yield_stable"] = lpps["LP_Pool_total_yield_stable"] + lpps_prev["LP_Pool_total_yield_stable"]
    lpps["LP_Pool_total_yield_asset"] = lpps["LP_Pool_total_yield_asset"] + lpps_prev["LP_Pool_total_yield_asset"]
    lpps["SYS_LP_Pool_TV_IntDep_stable"] = lpps["SYS_LP_Pool_TV_IntDep_stable"] + lpps_prev["SYS_LP_Pool_TV_IntDep_stable"]
    lpps["LP_Pool_total_deposited_stable"] = lpps["LP_Pool_total_deposited_stable"] + lpps_prev["LP_Pool_total_deposited_stable"]
    lpps["LP_Pool_total_deposited_asset"] = lpps["LP_Pool_total_deposited_asset"] + lpps_prev["LP_Pool_total_deposited_asset"]
    #lpps["SYS_LS_interest"] = lpps["SYS_LS_interest"] + lpps_prev["SYS_LS_interest"]
    #lpps["SYS_LS_Pool_interest"] = lpps["SYS_LS_Pool_interest"] + lpps_prev["SYS_LS_Pool_interest"]
    #lpps["SYS_LS_Pool_interest"] = lpps["SYS_LS_Pool_interest"] + lpps_prev["SYS_LS_Pool_interest"]
    return lpps


def lpps_create_record(timestamp, lpps_prev, LS_Opening, LP_Deposit, MP_Asset, LP_Pool, args):
    lpps = pd.DataFrame({"LP_Pool_id":LP_Pool["LP_Pool_id"],"LP_Pool_timestamp":np.repeat(timestamp,len(LP_Pool)),
                         "LP_Pool_total_value_locked_stable":np.repeat(0,len(LP_Pool)),"LP_Pool_total_value_locked_asset":np.repeat(0,len(LP_Pool)),"LP_Pool_total_issued_receipts":np.repeat(0,len(LP_Pool)),
                         "LP_Pool_total_borrowed_stable":np.repeat(0,len(LP_Pool)),"LP_Pool_total_borrowed_asset":np.repeat(0,len(LP_Pool)),"LP_Pool_total_yield_stable":np.repeat(0,len(LP_Pool)),
                                  "LP_Pool_total_yield_asset":np.repeat(0,len(LP_Pool)),"SYS_LP_Pool_TV_IntDep_stable":np.repeat(0,len(LP_Pool)),"LP_Pool_total_deposited_stable":np.repeat(0,len(LP_Pool)),
                         "LP_Pool_total_deposited_asset":np.repeat(0,len(LP_Pool)),"SYS_LS_interest":np.repeat(0,len(LP_Pool)),"SYS_LS_Pool_interest":np.repeat(0,len(LP_Pool)),"SYS_TR_interest":np.repeat(0,len(LP_Pool))})
    m = MP_Asset.loc[MP_Asset["MP_timestamp"] == timestamp]
    m = m.rename(columns={"MP_asset_symbol": "LP_symbol"})
    m["MP_price_in_stable"] = m["MP_price_in_stable"] / m.loc[
        m["LP_symbol"] == args["currency_stable"], "MP_price_in_stable"].values[0]
    m["LP_symbol"] = m["LP_symbol"].map(dict(LP_Pool[["LP_symbol","LP_Pool_id"]].values))

    borrowed_all,borrowed_asset = get_borrowed(timestamp,m,LS_Opening,LP_Pool,args)
    deposited_all, deposited_asset = get_deposited(timestamp,m,LP_Deposit,LP_Pool,args)
    #check
    try:
        lpps["LP_Pool_total_borrowed_stable"] = lpps["LP_Pool_id"].map(dict(borrowed_all[["LP_Pool_id","LS_loan_amnt_stable"]].values)).fillna(0)
        lpps["LP_Pool_total_borrowed_asset"] = lpps["LP_Pool_id"].map(dict(borrowed_asset[["LP_Pool_id","LS_loan_amnt_stable"]].values)).fillna(0)
    except:
        pass
    try:
        lpps["LP_Pool_total_deposited_stable"] = lpps["LP_Pool_id"].map(dict(deposited_all[["LP_Pool_id","LP_amnt_stable"]].values)).fillna(0)
        lpps["LP_Pool_total_deposited_asset"] = lpps["LP_Pool_id"].map(dict(deposited_asset[["LP_Pool_id","LP_amnt_stable"]].values)).fillna(0)
        lpps["SYS_LP_Pool_TV_IntDep_stable"] = lpps["LP_Pool_id"].map(dict(deposited_all[["LP_Pool_id","LP_amnt_stable"]].values)).fillna(0)
    except:
        pass
    lpps = sum_tables(lpps,lpps_prev)
    return lpps


def LP_Pool_State_update(timestamp, lpps_prev, LP_Pool_State, LS_Opening, LP_Deposit, MP_Asset, LP_Pool,repayment_sum,liquidation_sum,withdraw_sum,repayment_principal, args):

    lpps = lpps_create_record(timestamp,lpps_prev,LS_Opening,LP_Deposit,MP_Asset,LP_Pool,args)
    lpps = calculate_borrowed_tvl(repayment_sum, liquidation_sum, withdraw_sum, repayment_principal, lpps, args)

    LP_Pool_State_curr = lpps
    LP_Pool_State = LP_Pool_State.drop(LP_Pool_State.loc[LP_Pool_State["LP_Pool_timestamp"] == timestamp].index, axis=0)
    LP_Pool_State = pd.concat([LP_Pool_State, lpps], axis=0)
    return LP_Pool_State_curr, LP_Pool_State


def calculate_borrowed_tvl(repayment_sum,liquidation_sum,withdraw_sum,repayment_principal,lpps,args):#repayment pr
    borrowed = lpps[["LP_Pool_id", "LP_Pool_total_borrowed_stable"]].reset_index(drop=True)
    deposited = lpps[["LP_Pool_id", "SYS_LP_Pool_TV_IntDep_stable"]].reset_index(drop=True)
    borrowed["LP_Pool_total_borrowed_stable"] = borrowed["LP_Pool_total_borrowed_stable"] - borrowed["LP_Pool_id"].map(repayment_principal)
    #?>???> rep_sum = rep_principal + interest
    lpps["SYS_LS_interest"] = lpps["LP_Pool_id"].map(repayment_sum) + lpps["LP_Pool_id"].map(liquidation_sum)

    lpps["SYS_LS_Pool_interest"] =lpps["SYS_LS_interest"] * (1 - args["treasury_interest"] / 100)

    lpps["SYS_TR_interest"] =lpps["SYS_LS_interest"] * (args["treasury_interest"] / 100)

    pool_interest_value = lpps[["LP_Pool_id", "SYS_LS_Pool_interest"]].reset_index(drop=True)
    lpps["SYS_LP_Pool_TV_IntDep_stable"] =deposited["SYS_LP_Pool_TV_IntDep_stable"] + pool_interest_value[
                                                        "SYS_LS_Pool_interest"] - (
                                                        deposited["LP_Pool_id"].map(withdraw_sum))
    #LP_Pool_State.loc[cond, ["SYS_LP_Pool_TV_IntDep_stable"]] = LP_Pool_State.loc[cond, "LP_Pool_id"].map(dict(sum_deposited.values))
    return lpps