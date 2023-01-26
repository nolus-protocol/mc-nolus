import numpy as np
import pandas as pd

def PL_State_ini(MP_Asset):
    timestamp = MP_Asset.drop_duplicates(subset=["MP_timestamp"], keep="last")
    PL_State = pd.DataFrame()
    PL_State["PL_timestamp"] = timestamp["MP_timestamp"]

    return  PL_State

def count_in_time(timestamp,PL_State, to_count, name="count", timestamp_col_name="timestamp"):
    count = to_count.loc[to_count[timestamp_col_name]==timestamp,timestamp_col_name].count().astype("uint64")
    PL_State.loc[PL_State["PL_timestamp"]==timestamp,[name]]=count
    return PL_State

def PL_State_update(prev_timestamp,timestamp,PL_State,LS_Opening,LS_Repayment,LS_Liquidation,LS_Closing,LP_Deposit,LP_Withdraw,args):#put_out_closing
    present_cond = PL_State["PL_timestamp"]==timestamp
    past_cond = PL_State["PL_timestamp"]==prev_timestamp
    if prev_timestamp == None:
        prev_timestamp = timestamp
    present_cond = PL_State["PL_timestamp"]==timestamp
    past_cond = PL_State["PL_timestamp"]==prev_timestamp

    PL_State = count_in_time(timestamp,PL_State, LS_Opening,"PL_LS_count_opened","LS_timestamp")
    PL_State = count_in_time(timestamp,PL_State, LP_Deposit,"PL_LP_count_opened","LP_timestamp")
    PL_State = count_in_time(timestamp,PL_State, LS_Closing,"PL_LS_count_closed","LS_timestamp")
    PL_State = count_in_time(timestamp,PL_State, LP_Withdraw,"PL_LP_count_closed","LP_timestamp")
    if prev_timestamp == timestamp:
        PL_State.loc[present_cond, ["SYS_PL_LS_count_open_sum"]]=0
        PL_State.loc[present_cond, ["SYS_PL_LP_count_open_sum"]]=0
        PL_State.loc[present_cond, ["SYS_PL_LS_count_closed_sum"]]=0
        PL_State.loc[present_cond, ["SYS_PL_LP_count_closed_sum"]]=0
    else:
        PL_State.loc[present_cond,["SYS_PL_LS_count_open_sum"]] = PL_State.loc[past_cond,"SYS_PL_LS_count_open_sum"].values + PL_State.loc[present_cond,"PL_LS_count_opened"].values
        PL_State.loc[present_cond,["SYS_PL_LP_count_open_sum"]] = PL_State.loc[past_cond,["SYS_PL_LP_count_open_sum"]].values + PL_State.loc[present_cond,["PL_LP_count_opened"]].values
        PL_State.loc[present_cond,["SYS_PL_LS_count_closed_sum"]] = PL_State.loc[past_cond,["SYS_PL_LS_count_closed_sum"]].values + PL_State.loc[present_cond,["PL_LS_count_closed"]].values
        PL_State.loc[present_cond,["SYS_PL_LP_count_closed_sum"]] = PL_State.loc[past_cond,["SYS_PL_LP_count_closed_sum"]].values + PL_State.loc[present_cond,["PL_LP_count_closed"]].values

    PL_State.loc[present_cond,["PL_LS_count_open"]] = PL_State.loc[present_cond,["SYS_PL_LS_count_open_sum"]].values - PL_State.loc[present_cond,["SYS_PL_LS_count_closed_sum"]].values
    PL_State.loc[present_cond,["PL_LP_count_open"]] = PL_State.loc[present_cond,["SYS_PL_LP_count_open_sum"]].values - PL_State.loc[present_cond,["SYS_PL_LP_count_closed_sum"]].values

    return PL_State


def PL_State_finalize(nolus_price,PL_State, LP_Pool_state, LS_Opening, LS_Repayment, LS_Closing, LP_Deposit, LP_Withdraw, TR_Profit,
                      TR_Rewards_Distribution, PL_Interest, args):
    borrowed = LP_Pool_state[
        ["LP_Pool_timestamp", "LP_Pool_id", "LP_Pool_total_borrowed_stable", "SYS_LP_Pool_TV_IntDep_stable"]]
    borrowed = borrowed.groupby("LP_Pool_timestamp").sum().reset_index()
    PL_State["PL_pools_TVL_stable"] = borrowed["LP_Pool_total_borrowed_stable"] + borrowed[
        "SYS_LP_Pool_TV_IntDep_stable"]
    PL_State["PL_pools_borrowed_stable"] = borrowed["LP_Pool_total_borrowed_stable"]
    PL_State["PL_pools_yield_stable"] = 0

    temp = LS_Opening[["LS_timestamp", "LS_cltr_amnt_stable"]].groupby("LS_timestamp").sum().reset_index()
    PL_State["PL_IN_LS_cltr_amnt_opened_stable"] = PL_State["PL_timestamp"].map(dict(temp.values))

    temp = LS_Opening[["LS_timestamp", "LS_loan_amnt_stable"]].groupby("LS_timestamp").sum().reset_index()
    PL_State["PL_OUT_LS_loan_amnt_stable"] = PL_State["PL_timestamp"].map(dict(temp.values))

    a = LS_Repayment[["LS_timestamp", "LS_prev_margin_stable"]].groupby("LS_timestamp").sum().reset_index()
    PL_State["PL_IN_LS_rep_prev_margin_stable"] = PL_State["PL_timestamp"].map(dict(a.values))
    PL_State["PL_IN_LS_rep_prev_margin_stable"] = PL_State["PL_IN_LS_rep_prev_margin_stable"].fillna(0)

    a = LS_Repayment[["LS_timestamp", "LS_prev_interest_stable"]].groupby("LS_timestamp").sum().reset_index()
    PL_State["PL_IN_LS_rep_prev_interest_stable"] = PL_State["PL_timestamp"].map(dict(a.values))
    PL_State["PL_IN_LS_rep_prev_interest_stable"] = PL_State["PL_IN_LS_rep_prev_interest_stable"].fillna(0)

    a = LS_Repayment[["LS_timestamp", "LS_current_margin_stable"]].groupby("LS_timestamp").sum().reset_index()
    PL_State["PL_IN_LS_rep_current_margin_stable"] = PL_State["PL_timestamp"].map(dict(a.values))
    PL_State["PL_IN_LS_rep_current_margin_stable"] = PL_State["PL_IN_LS_rep_current_margin_stable"].fillna(0)

    a = LS_Repayment[["LS_timestamp", "LS_current_interest_stable"]].groupby("LS_timestamp").sum().reset_index()
    PL_State["PL_IN_LS_rep_current_interest_stable"] = PL_State["PL_timestamp"].map(dict(a.values))
    PL_State["PL_IN_LS_rep_current_interest_stable"] = PL_State["PL_IN_LS_rep_current_interest_stable"].fillna(0)

    a = LS_Repayment[["LS_timestamp", "LS_principal_stable"]].groupby("LS_timestamp").sum().reset_index()
    PL_State["PL_IN_LS_rep_principal_stable"] = PL_State["PL_timestamp"].map(dict(a.values))
    PL_State["PL_IN_LS_rep_principal_stable"] = PL_State["PL_IN_LS_rep_principal_stable"].fillna(0)

    PL_State["PL_IN_LS_rep_amnt_stable"] = PL_State["PL_IN_LS_rep_principal_stable"] + PL_State[
        "PL_IN_LS_rep_current_interest_stable"] + PL_State["PL_IN_LS_rep_current_margin_stable"] + PL_State[
                                               "PL_IN_LS_rep_prev_interest_stable"] + PL_State[
                                               "PL_IN_LS_rep_prev_margin_stable"]

    LS_Closing["SYS_PL_OUT_LS_cltr_amnt_stable"] = LS_Closing["LS_cltr_amnt_out"]
    #todo: divide by symbol_digit_for_cltr
    a = LS_Closing[["LS_timestamp", "SYS_PL_OUT_LS_cltr_amnt_stable"]].groupby("LS_timestamp").sum().reset_index()
    PL_State["PL_OUT_LS_cltr_amnt_stable"] = PL_State["PL_timestamp"].map(dict(a.values))
    PL_State["PL_OUT_LS_cltr_amnt_stable"] = PL_State["PL_OUT_LS_cltr_amnt_stable"].fillna(0)

    PL_State["PL_OUT_LS_amnt_stable"] = PL_State["PL_OUT_LS_cltr_amnt_stable"]  + PL_State["PL_OUT_LS_loan_amnt_stable"]
    PL_State["PL_native_amnt_stable"] = 0
    PL_State["PL_native_amnt_nolus"] = 0

    a = LP_Deposit[["LP_timestamp", "LP_amnt_stable"]].groupby("LP_timestamp").sum().reset_index()
    PL_State["PL_IN_LP_amnt_stable"] = PL_State["PL_timestamp"].map(dict(a.values))
    PL_State["PL_IN_LP_amnt_stable"] = PL_State["PL_IN_LP_amnt_stable"].fillna(0)

    a = LP_Withdraw[["LP_timestamp", "LP_amnt_stable"]].groupby("LP_timestamp").sum().reset_index()
    PL_State["PL_OUT_LP_amnt_stable"] = PL_State["PL_timestamp"].map(dict(a.values))
    PL_State["PL_OUT_LP_amnt_stable"] = PL_State["PL_OUT_LP_amnt_stable"].fillna(0)

    temp = TR_Profit[["TR_Profit_timestamp", "TR_Profit_amnt_stable", "TR_Profit_amnt_nls"]]
    temp = temp.rename(
        columns={"TR_Profit_timestamp": "PL_timestamp", "TR_Profit_amnt_stable": "PL_TR_profit_amnt_stable",
                 "TR_Profit_amnt_nls": "PL_TR_profit_amnt_nls"})
    PL_State = pd.merge(PL_State, temp, on="PL_timestamp", how="left")
    PL_State["PL_OUT_TR_rewards_amnt_stable"] = \
    TR_Rewards_Distribution[["TR_Rewards_timestamp", "TR_Rewards_amnt_stable"]].groupby(
        "TR_Rewards_timestamp").sum().reset_index()["TR_Rewards_amnt_stable"]
    PL_State["PL_OUT_TR_rewards_amnt_nls"] = \
    TR_Rewards_Distribution[["TR_Rewards_timestamp", "TR_Rewards_amnt_nls"]].groupby(
        "TR_Rewards_timestamp").sum().reset_index()["TR_Rewards_amnt_nls"]
    PL_State["PL_TR_tax_amnt_stable"] = PL_State[["PL_TR_tax_amnt_stable"]].fillna(0)
    PL_State["PL_TR_tax_amnt_nls"] = PL_State["PL_TR_tax_amnt_stable"] * PL_State["PL_timestamp"].map(dict(nolus_price[["MP_timestamp","MP_price_in_stable"]].values))#args["nolus_token_price_ini"]
    a = PL_Interest[["PL_timestamp","Util"]].groupby("PL_timestamp").sum().reset_index()
    PL_State["SYS_PL_Utilization"] = PL_State["PL_timestamp"].map(dict(a.values))*100
    PL_State["SYS_PL_Utilization"] = PL_State["SYS_PL_Utilization"]/len(args["Pool_Assets"])
    return PL_State



