import numpy as np
import pandas as pd

def TR_Profit_ini():
    TR_Profit = pd.DataFrame({"TR_Profit_height":[],"TR_Profit_idx":[],"TR_Profit_timestamp":[],"TR_Profit_amnt_stable":[],"TR_Profit_amnt_nls":[]})
    return TR_Profit


def tr_profit_amnt_stable(timestamp, i, c, rewards_distributed_amnt, LP_Pool_State, PL_State, LS_Opening, args):#rewards_distributed_amnt  - distributet amnt in stable for all pools summed
    var = pd.DataFrame()
    if i == 0:
        a = pd.DataFrame(args["symbol_digit"])
        c = a.loc[a["symbol"] == args["currency_stable"]]["digit"].values[0].astype(float)
        var = args["nolus_token_count_ini"] *args["nolus_token_price_ini"] * 10 ** c
        return var
    lpps = LP_Pool_State.loc[LP_Pool_State["LP_Pool_timestamp"]==timestamp]
    lso = LS_Opening.loc[LS_Opening["LS_timestamp"]==timestamp]
    pls = PL_State.loc[PL_State["PL_timestamp"]== timestamp]
    all_profit = pd.DataFrame()
    var = lpps[["LP_Pool_timestamp", "SYS_TR_interest"]].groupby("LP_Pool_timestamp").sum().reset_index()

    var = var.rename(columns={"LP_Pool_timestamp": "TR_Profit_timestamp"})

    swap_profit = lso[["LS_timestamp", "LS_loan_amnt_stable"]].groupby("LS_timestamp").sum().reset_index()
    swap_profit["LS_loan_amnt_stable"] = swap_profit["LS_loan_amnt_stable"] * (args["LS_swap_prc"] / 100)
    swap_profit = swap_profit.rename(columns={"LS_timestamp": "TR_Profit_timestamp"})

    ls_profit = pls[["PL_timestamp", "PL_LS_count_open"]]
    ls_profit["ls_profit"] = ls_profit["PL_LS_count_open"] * (args["LS_tr_per_month"] / 30) * args["tr_price"]
    ls_profit = ls_profit.rename(columns={"PL_timestamp": "TR_Profit_timestamp"})
    ls_profit = ls_profit.drop("PL_LS_count_open", axis=1)
    lp_profit = pls[["PL_timestamp", "PL_LP_count_open"]]
    lp_profit["lp_profit"] = lp_profit["PL_LP_count_open"] * (args["LP_tr_per_month"] / 30) * args["tr_price"]
    lp_profit = lp_profit.rename(columns={"PL_timestamp": "TR_Profit_timestamp"})
    lp_profit = lp_profit.drop("PL_LP_count_open", axis=1)
    all_profit["PL_timestamp"] = timestamp
    all_profit["profit"] = lp_profit["lp_profit"] + ls_profit["ls_profit"]
    all_profit["PL_timestamp"] = timestamp
    PL_State.loc[PL_State["PL_timestamp"] == timestamp, "PL_TR_tax_amnt_stable"] = PL_State.loc[PL_State["PL_timestamp"] == timestamp, "PL_timestamp"].map(dict(all_profit.values))
    var = pd.merge(var, swap_profit, on=["TR_Profit_timestamp"], how="left")
    var = pd.merge(var, ls_profit, on=["TR_Profit_timestamp"], how="left")
    var = pd.merge(var, lp_profit, on=["TR_Profit_timestamp"], how="left")
    var = var.fillna(0)
    var["all_tr_profit"] = var["ls_profit"]*10**c + var["lp_profit"]*10**c + var["LS_loan_amnt_stable"] + var["SYS_TR_interest"] - rewards_distributed_amnt


    return var["all_tr_profit"].values



def TR_Profit_update(timestamp, i,rewards_distributed_amnt,nolus_price, TR_Profit, LP_Pool_State, PL_State, LS_Opening, args):
    a = pd.DataFrame(args["symbol_digit"])
    c = a.loc[a["symbol"] == args["currency_stable"]]["digit"].values[0].astype(float)
    amnt_stable = tr_profit_amnt_stable(timestamp,i,c,rewards_distributed_amnt, LP_Pool_State, PL_State, LS_Opening, args)
    temp = pd.DataFrame({"TR_Profit_height":(100+i),"TR_Profit_idx":(100+i),"TR_Profit_timestamp":timestamp,"TR_Profit_amnt_stable":amnt_stable,"TR_Profit_amnt_nls":amnt_stable/nolus_price.loc[nolus_price["MP_timestamp"]==timestamp]["MP_price_in_stable"].values},index=[0])#args["nolus_token_price_ini"]},index=[0])
    temp["TR_Profit_amnt_nls"] = temp["TR_Profit_amnt_nls"]/10**c
    TR_Profit = TR_Profit.drop(TR_Profit.loc[TR_Profit["TR_Profit_timestamp"] == timestamp].index)
    TR_Profit = pd.concat([TR_Profit, temp], axis=0, ignore_index=True)
    return TR_Profit

