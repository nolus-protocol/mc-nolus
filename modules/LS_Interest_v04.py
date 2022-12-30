import pandas as pd
import numpy as np
import warnings
from modules.PL_CurrentState import PL_State_update
from modules.TR_Profit import TR_Profit_update
from modules.TR_Rewards_Distribution import TR_Rewards_Distribution_update
from modules.TR_State import TR_State_update
from modules.LS_State import LS_State_update
from modules.LS_Closing import LS_Closing_update

warnings.filterwarnings('ignore')

# with open("config.json", 'r') as f:
#     args = json.load(f)
# with open("MP_ASSET_STATE.csv", "r") as outfile:
#     MP_Asset_State = pd.read_csv(outfile, index_col=0)
# with open("MP_ASSET.csv", "r") as outfile:
#     MP_Asset = pd.read_csv(outfile, index_col=0)
#
# LS_Opening = pd.read_csv("LS_Opening", index_col=0)
# LP_Deposit = pd.read_csv("LP_Deposit", index_col=0)
# LP_Withdraw = pd.read_csv("LP_Withdraw", index_col=0)
# SYS_LP_Withdraw = pd.read_csv("SYS_LP_Withdraw", index_col=0)
#
# LS_Repayment = pd.read_csv("LS_Repayment", index_col=0)
# LS_Closing = pd.read_csv("LS_Closing", index_col=0)
# LS_Liquidation = pd.read_csv("LS_Liquidation", index_col=0)
# pool_id = lpps.LP_pool_gen(args)
# pool_id


def get_timestamps(MP_Asset):
    timestamps = pd.DataFrame()
    timestamps = MP_Asset.drop_duplicates(subset=["MP_timestamp"])
    timestamps = pd.DataFrame(timestamps["MP_timestamp"])
    timestamps.rename(columns={"MP_timestamp": "PL_Interest_timestamp"})
    return timestamps

def calculate_interest(timestamp, LP_Pool_State, pool_util, args):
    # util = pd["id":Util]

    pools = LP_Pool_State.loc[LP_Pool_State["LP_Pool_timestamp"] == timestamp]
    pools = pools[["LP_Pool_id", "LP_Pool_total_borrowed_stable", "SYS_LP_Pool_TV_IntDep_stable"]]
    # pools = pools.rename(columns={"LP_Pool_id":"id"})
    pools = pd.merge(pools, pool_util, on="LP_Pool_id", how="left")
    util = (pools["LP_Pool_total_borrowed_stable"] / (
            pools["SYS_LP_Pool_TV_IntDep_stable"] + pools["LP_Pool_total_borrowed_stable"]))
    pools.loc[pools["Util"] <= args["optimal_util"] / 100, ["interest"]] = args["base_interest"] / 100 + (
            util / (args["optimal_util"] / 100)) * (args["slope1"] / 100)
    pools.loc[pools["Util"] > args["optimal_util"] / 100, ["interest"]] = args["base_interest"] / 100 + args[
        "slope1"] / 100 + (((util - args["optimal_util"] / 100) / (1 - args["optimal_util"] / 100)) * args[
        "slope2"] / 100)
    # HARDCODE CAP
    pools.loc[pools["interest"] > args["LS_interest_cap"] / 100, ["interest"]] = args["LS_interest_cap"] / 100
    pool_util["Util"] = util

    pools = pools.drop(["LP_Pool_total_borrowed_stable", "SYS_LP_Pool_TV_IntDep_stable", "Util"], axis=1)
    return pools, pool_util


def LS_int_update(LS_Repayment, LS_Liquidation, rep, liq, rep_val, liq_val, principal, ls_curr_margins, ls_curr_interest,
                  ls_prev_interest=None, ls_prev_margins=None, args=None):
    LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(rep["LS_contract_id"]), "LS_amnt_stable"] = LS_Repayment.loc[
        LS_Repayment["LS_contract_id"].isin(rep["LS_contract_id"]), "LS_contract_id"].map(rep_val)

    LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(principal["LS_contract_id"]), "LS_principal_stable"] = \
        LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(principal["LS_contract_id"]), "LS_contract_id"].map(
            dict(principal.values))

    LS_Repayment.loc[
        LS_Repayment["LS_contract_id"].isin(ls_curr_margins["LS_contract_id"]), "LS_current_margin_stable"] = \
        LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(ls_curr_margins["LS_contract_id"]), "LS_contract_id"].map(
            dict(ls_curr_margins.values))

    LS_Repayment.loc[
        LS_Repayment["LS_contract_id"].isin(ls_curr_interest["LS_contract_id"]), "LS_current_interest_stable"] = \
        LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(ls_curr_interest["LS_contract_id"]), "LS_contract_id"].map(
            dict(ls_curr_interest.values))

    LS_Repayment.loc[
        LS_Repayment["LS_contract_id"].isin(ls_prev_interest["LS_contract_id"]), "LS_prev_interest_stable"] = \
        LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(ls_prev_interest["LS_contract_id"]), "LS_contract_id"].map(
            dict(ls_prev_interest.values))
    LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(ls_prev_margins["LS_contract_id"]), "LS_prev_margin_stable"] = \
        LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(ls_prev_margins["LS_contract_id"]), "LS_contract_id"].map(
            dict(ls_prev_margins.values))

    LS_Liquidation.loc[LS_Liquidation["LS_contract_id"].isin(liq["LS_contract_id"]), "LS_amnt_stable"] = \
        LS_Liquidation.loc[LS_Liquidation["LS_contract_id"].isin(liq["LS_contract_id"]), "LS_contract_id"].map(liq_val)

    LS_Liquidation.loc[LS_Liquidation["LS_contract_id"].isin(principal["LS_contract_id"]), "LS_principal_stable"] = \
        LS_Liquidation.loc[LS_Liquidation["LS_contract_id"].isin(principal["LS_contract_id"]), "LS_contract_id"].map(
            dict(principal.values))
    LS_Liquidation.loc[
        LS_Liquidation["LS_contract_id"].isin(ls_curr_margins["LS_contract_id"]), "LS_current_margin_stable"] = \
        LS_Liquidation.loc[
            LS_Liquidation["LS_contract_id"].isin(ls_curr_margins["LS_contract_id"]), "LS_contract_id"].map(
            dict(ls_curr_margins.values))
    LS_Liquidation.loc[
        LS_Liquidation["LS_contract_id"].isin(ls_curr_interest["LS_contract_id"]), "LS_current_interest_stable"] = \
        LS_Liquidation.loc[
            LS_Liquidation["LS_contract_id"].isin(ls_curr_interest["LS_contract_id"]), "LS_contract_id"].map(
            dict(ls_curr_interest.values))
    LS_Liquidation.loc[
        LS_Liquidation["LS_contract_id"].isin(ls_prev_interest["LS_contract_id"]), "LS_prev_interest_stable"] = \
        LS_Liquidation.loc[
            LS_Liquidation["LS_contract_id"].isin(ls_prev_interest["LS_contract_id"]), "LS_contract_id"].map(
            dict(ls_prev_interest.values))
    LS_Liquidation.loc[
        LS_Liquidation["LS_contract_id"].isin(ls_prev_margins["LS_contract_id"]), "LS_prev_margin_stable"] = \
        LS_Liquidation.loc[
            LS_Liquidation["LS_contract_id"].isin(ls_prev_margins["LS_contract_id"]), "LS_contract_id"].map(
            dict(ls_prev_margins.values))

    # todo LS_Liquidation["SYS_cltr_taken"] = amnt_taken(interest) * "SYS_LS_cltr_price" /10**currency_stable_digit * 10**currency_asset_digit
    liq_cond = LS_Liquidation["LS_contract_id"].isin(liq["LS_contract_id"])
    LS_Liquidation.loc[liq_cond, "SYS_LS_cltr_amnt_taken"] = LS_Liquidation.loc[liq_cond, "LS_amnt_stable"] / LS_Liquidation.loc[liq_cond, "SYS_LS_cltr_price"]
    a = pd.DataFrame(args["symbol_digit"])
    a = a.rename(columns={"symbol": "SYS_LS_asset_symbol"})
    LS_Liquidation = pd.merge(LS_Liquidation, a, on="SYS_LS_asset_symbol", how="left")
    #LS_Liquidation["SYS_LS_cltr_amnt_taken"] = LS_Liquidation["SYS_LS_cltr_amnt_taken"]*10 ** LS_Liquidation["digit"].fillna(0).round(0).astype("uint64")
    LS_Liquidation.loc[liq_cond, "SYS_LS_cltr_amnt_taken"] = LS_Liquidation.loc[liq_cond, ["SYS_LS_cltr_amnt_taken"]]*10**LS_Liquidation.loc[liq_cond, "digit"].fillna(0).round(0).astype("uint64")

    LS_Liquidation = LS_Liquidation.drop("digit", axis=1)
    return LS_Repayment,LS_Liquidation

def LS_fill_interest(timestamp,prev_timestamp,open_contracts, pool_interest,MP_Asset, LS_Opening, LS_Repayment, LS_Liquidation,LS_State,LS_Closing, pool_id,symbol_id, args):
    contract_ids = LS_Opening[["LS_contract_id", "LS_loan_amnt_stable", "LP_Pool_id", "SYS_LS_expected_payment",
                               "SYS_LS_expected_penalty"]].loc[LS_Opening["LS_timestamp"] == timestamp]
    p_i = dict(pool_interest.values)
    LS_Opening["LS_interest"].loc[LS_Opening["LS_timestamp"] == timestamp] = LS_Opening["LP_Pool_id"].loc[
        LS_Opening["LS_timestamp"] == timestamp].map(p_i)
    #amnt = contract_ids["LS_loan_amnt_stable"]
    #payment = contract_ids["SYS_LS_expected_payment"]
    #penalty = contract_ids["SYS_LS_expected_penalty"]
    #pool = contract_ids["LP_Pool_id"]
    contract_ids = pd.merge(contract_ids, pool_interest, on="LP_Pool_id", how='left')
    contract_ids["repayment_amnt"] = contract_ids["LS_loan_amnt_stable"] * (
            1 + contract_ids["interest"] * (1 - contract_ids["SYS_LS_expected_penalty"] / 12)) / (
                                             contract_ids["SYS_LS_expected_payment"] + contract_ids[
                                         "SYS_LS_expected_penalty"])

    contract_ids["liquidation_amnt"] = contract_ids["LS_loan_amnt_stable"] * (
        (contract_ids["interest"] * contract_ids["SYS_LS_expected_penalty"] / 12)) / (
                                               contract_ids["SYS_LS_expected_payment"] + contract_ids[
                                           "SYS_LS_expected_penalty"])
    contract_ids["ls_principal_stable"] = contract_ids["LS_loan_amnt_stable"] / (contract_ids["SYS_LS_expected_payment"] + contract_ids[
                                           "SYS_LS_expected_penalty"])
    contract_ids["ls_rep_margins"] = (contract_ids["repayment_amnt"]-contract_ids["ls_principal_stable"])/ 4

    contract_ids = contract_ids.drop(
        ["LS_loan_amnt_stable", "LP_Pool_id", "SYS_LS_expected_payment", "SYS_LS_expected_penalty"], axis=1)
    rep = contract_ids[["LS_contract_id", "repayment_amnt"]]
    rep = rep.rename(columns={"repayment_amnt": "LS_amnt_stable"})
    rep_val = dict(rep.values)
    liq = contract_ids[["LS_contract_id", "liquidation_amnt"]]
    liq = liq.rename(columns={"liquidation_amnt": "LS_amnt_stable"})
    liq_val = dict(liq.values)
    principal = contract_ids[["LS_contract_id", "ls_principal_stable"]]
    principal = principal.rename(columns={"ls_principal_stable": "LS_principal_stable"})
    margins = contract_ids[["LS_contract_id", "ls_rep_margins"]]
    ls_curr_margins = margins.rename(columns={"ls_rep_margins": "LS_current_margin_stable"})
    ls_curr_interest = margins.rename(columns={"ls_rep_margins": "LS_current_interest_stable"})
    ls_prev_interest = margins.rename(columns={"ls_rep_margins": "LS_prev_interest_stable"})
    ls_prev_margins = margins.rename(columns={"ls_rep_margins": "LS_prev_margin_stable"})
    # updates ---
    LS_Repayment,LS_Liquidation = LS_int_update(LS_Repayment, LS_Liquidation, rep, liq, rep_val, liq_val, principal, ls_curr_margins,
                  ls_curr_interest,
                  ls_prev_interest, ls_prev_margins, args)

    #ls_amnt_stable from ls_liquidation -> SYSLLS_Cltr_taken_stable

    open_contracts,LS_State,LS_Repayment,LS_Liquidation,LS_Closing = LS_State_update(timestamp, prev_timestamp,
                                                                                             open_contracts, MP_Asset,
                                                                                             LS_State, LS_Opening,
                                                                                             LS_Repayment,
                                                                                             LS_Liquidation, LS_Closing,
                                                                                             args)

    #todo : end of the updating function

    repayment_sum = LS_Repayment.loc[LS_Repayment["LS_timestamp"] == timestamp, ["LS_symbol", "LS_amnt_stable"]]
    liquidation_sum = LS_Liquidation.loc[LS_Liquidation["LS_timestamp"] == timestamp, ["LS_symbol", "LS_amnt_stable"]]
    #todo: import liquidation_sum_type_2
    repayment_principal = LS_Repayment.loc[LS_Repayment["LS_timestamp"] == timestamp, ["LS_symbol","LS_principal_stable"]]


    repayment_sum = repayment_sum.groupby("LS_symbol")["LS_amnt_stable"].sum()
    liquidation_sum = liquidation_sum.groupby("LS_symbol")["LS_amnt_stable"].sum()
    repayment_principal = repayment_principal.groupby("LS_symbol")["LS_principal_stable"].sum()


    if repayment_sum.empty == True:
        repayment_sum = pd.DataFrame({"LS_symbol": args["Pool_Assets"], "LS_amnt_stable": 0})
    else:
        repayment_sum = repayment_sum.reset_index()
        repayment_sum = repayment_sum.fillna(0)
    if liquidation_sum.empty == True:
        liquidation_sum = pd.DataFrame({"LS_symbol": args["Pool_Assets"], "LS_amnt_stable": 0})
    else:
        liquidation_sum = liquidation_sum.reset_index()
        liquidation_sum = liquidation_sum.fillna(0)
    if repayment_principal.empty == True:
        repayment_principal = pd.DataFrame({"LS_symbol": args["Pool_Assets"], "LS_principal_stable": 0})
    else:
        repayment_principal = repayment_principal.reset_index()
        repayment_principal = repayment_principal.fillna(0)


    repayment_sum["LP_Pool_id"] = repayment_sum["LS_symbol"].map(symbol_id)
    repayment_sum = repayment_sum.drop("LS_symbol", axis=1)
    liquidation_sum["LP_Pool_id"] = liquidation_sum["LS_symbol"].map(symbol_id)
    liquidation_sum = liquidation_sum.drop("LS_symbol", axis=1)
    repayment_principal["LP_Pool_id"] = repayment_principal["LS_symbol"].map(symbol_id)
    repayment_principal = repayment_principal.drop("LS_symbol",axis=1)
    return LS_Opening, LS_Repayment, LS_Liquidation,LS_State,LS_Closing,open_contracts, repayment_sum, liquidation_sum,repayment_principal

def fill_withdraw(timestamp, LP_Withdraw, LP_Deposit, SYS_LP_Withdraw):
 #   timestamp = ti
    if not LP_Withdraw.loc[LP_Withdraw["LP_timestamp"]==timestamp].empty:
        address_id = LP_Withdraw[["SYS_LP_contract_id", "LP_amnt_stable", "LP_Pool_id"]].loc[
            LP_Withdraw["LP_timestamp"] == timestamp]
        interest = SYS_LP_Withdraw[["SYS_LP_contract_id", "LP_interest_amnt"]]
        cond = interest["SYS_LP_contract_id"].isin(address_id["SYS_LP_contract_id"])
        interest = interest.loc[cond].groupby("SYS_LP_contract_id").apply(pd.Series.sum, skipna=True)
        interest = interest.drop("SYS_LP_contract_id", axis=1)
        interest = interest.reset_index()
        deposited = LP_Deposit.loc[LP_Deposit["SYS_LP_contract_id"].isin(address_id["SYS_LP_contract_id"])]
        deposited = deposited[["SYS_LP_contract_id", "LP_amnt_stable"]]
        deposited = dict(deposited.values)
        interest["base"] = interest["SYS_LP_contract_id"].map(deposited)
        interest["LP_amnt_stable"] = interest["base"] + interest["LP_interest_amnt"]
        interest = interest.drop(["base", "LP_interest_amnt"], axis=1)
        # print(interest)
        interest = dict(interest.values)
        LP_Withdraw.loc[LP_Withdraw["LP_timestamp"]==timestamp,"LP_amnt_stable"] = LP_Withdraw.loc[LP_Withdraw["LP_timestamp"]==timestamp,"SYS_LP_contract_id"].map(interest)
    return LP_Withdraw


def LP_fill_interest(timestamp, pool_interest, LP_Deposit, LP_Withdraw, SYS_LP_Withdraw, pool_id, args):
    # update - contract_id -> contract_id
    p_i = pool_interest
    p_i["interest"] = p_i["interest"] - args["treasury_interest"] / 100
    contract_id = SYS_LP_Withdraw[["SYS_LP_contract_id"]].loc[SYS_LP_Withdraw["LP_timestamp"] == timestamp]
    contract_id = LP_Deposit[["SYS_LP_contract_id", "LP_amnt_stable", "LP_Pool_id"]].loc[
        LP_Deposit["SYS_LP_contract_id"].isin(contract_id["SYS_LP_contract_id"])]
    amnt = contract_id[["SYS_LP_contract_id", "LP_amnt_stable"]]
    amnt = dict(amnt.values)
    pool = contract_id["LP_Pool_id"]

    contract_id = pd.merge(contract_id, p_i, on="LP_Pool_id", how='left')
    p_i = p_i.copy()
    p_i["interest"] = p_i["interest"]/365
    p_i = dict(p_i.values)
    SYS_LP_Withdraw.loc[SYS_LP_Withdraw["LP_timestamp"] == timestamp, "LP_interest"] = SYS_LP_Withdraw[
        "LP_Pool_id"].map(p_i)

    SYS_LP_Withdraw.loc[SYS_LP_Withdraw["LP_timestamp"] == timestamp, "LP_amnt_stable"] = \
    SYS_LP_Withdraw["SYS_LP_contract_id"].loc[SYS_LP_Withdraw["LP_timestamp"] == timestamp].map(amnt)

    # a = SYS_LP_Withdraw.loc[SYS_LP_Withdraw["LP_timestamp"] == timestamp]
    time = SYS_LP_Withdraw.loc[
        SYS_LP_Withdraw["LP_timestamp"] == timestamp, ["LP_amnt_stable", "LP_interest", "SYS_LP_contract_id"]]
    time["LP_interest_amnt"] = time["LP_amnt_stable"] * time[
                                                             "LP_interest"]
    time = time.drop(["LP_interest", "LP_amnt_stable"], axis=1)
    time = dict(time.values)
    SYS_LP_Withdraw.loc[SYS_LP_Withdraw["LP_timestamp"] == timestamp, "LP_interest_amnt"] = \
        SYS_LP_Withdraw["SYS_LP_contract_id"].loc[SYS_LP_Withdraw["LP_timestamp"] == timestamp].map(time)

    LP_Withdraw = fill_withdraw(timestamp, LP_Withdraw, LP_Deposit, SYS_LP_Withdraw)

    withdraw_sum = LP_Withdraw.loc[LP_Withdraw["LP_timestamp"] == timestamp, ["LP_Pool_id", "LP_amnt_stable"]]
    # print(withdraw_sum)
    withdraw_sum = withdraw_sum.groupby("LP_Pool_id")["LP_amnt_stable"].sum()
    if withdraw_sum.empty == True:
        withdraw_sum = pd.DataFrame({"LP_Pool_id": pool_id["LP_Pool_id"], "LS_amnt_stable": 0})
    else:
        withdraw_sum = withdraw_sum.reset_index()
        withdraw_sum = withdraw_sum.fillna(0)
    return LP_Deposit, LP_Withdraw, SYS_LP_Withdraw, withdraw_sum


def additions_check(withdraw_sum, liquidation_sum, repayment_sum,repayment_principal,pool_id):
    repayment_sum_model = pd.DataFrame({"LP_Pool_id": pool_id["LP_Pool_id"], "LS_amnt_stable": 0})
    liquidation_sum_model = pd.DataFrame({"LP_Pool_id": pool_id["LP_Pool_id"], "LS_amnt_stable": 0})
    withdraw_sum_model = pd.DataFrame({"LP_Pool_id": pool_id["LP_Pool_id"], "LP_amnt_stable": 0})
    repayment_principal_model = pd.DataFrame({"LP_Pool_id": pool_id["LP_Pool_id"], "LS_principal_stable": 0})


    try:
        liquidation_sum = {v: k for k, v in dict(liquidation_sum.values).items()}
        liquidation_sum_model["LS_amnt_stable"] = liquidation_sum_model["LP_Pool_id"].map(liquidation_sum).fillna(0)
    except:
        pass
    try:
        repayment_principal = {v: k for k, v in dict(repayment_principal.values).items()}
        repayment_principal_model["LS_principal_stable"] = repayment_principal_model["LP_Pool_id"].map(repayment_principal).fillna(0)
    except:
        pass

    try:
        repayment_sum = {v: k for k, v in dict(repayment_sum.values).items()}
        repayment_sum_model["LS_amnt_stable"] = repayment_sum_model["LP_Pool_id"].map(repayment_sum).fillna(0)
    except:
        pass

    try:
        withdraw_sum = dict(withdraw_sum.values)
        withdraw_sum_model["LP_amnt_stable"] = withdraw_sum_model["LP_Pool_id"].map(withdraw_sum).fillna(0)
    except:
        pass

    repayment_sum = dict(repayment_sum_model.values)
    liquidation_sum = dict(liquidation_sum_model.values)
    withdraw_sum = dict(withdraw_sum_model.values)
    repayment_principal = dict(repayment_principal_model.values)
    return withdraw_sum, liquidation_sum, repayment_sum,repayment_principal


def check_util(timestamp, pool_util, LS_Opening, LS_Repayment, LS_Liquidation, LP_Pool_State,borrowed, args):
    pid = pool_util.loc[pool_util["Util"] >= args["max_pool_util"] / 100]
    if pid.empty:
        return pool_util, LS_Opening, LS_Repayment, LS_Liquidation, LP_Pool_State, False

    contract_ids = LS_Opening[["LS_contract_id", "LP_Pool_id"]].loc[LS_Opening["LS_timestamp"] == timestamp]
    contract_ids = contract_ids.loc[contract_ids["LP_Pool_id"].isin(pid["LP_Pool_id"])]

    LS_Opening = LS_Opening.loc[~LS_Opening["LS_contract_id"].isin(contract_ids["LS_contract_id"])]
    LS_Repayment = LS_Repayment.loc[~LS_Repayment["LS_contract_id"].isin(contract_ids["LS_contract_id"])]
    LS_Liquidation = LS_Liquidation.loc[~LS_Liquidation["LS_contract_id"].isin(contract_ids["LS_contract_id"])]
    LP_Pool_State["LP_Pool_total_borrowed_stable"].loc[(LP_Pool_State["LP_Pool_timestamp"]==timestamp) & (LP_Pool_State["LP_Pool_id"].isin(pid["LP_Pool_id"]))] = 0
    LP_Pool_State["LP_Pool_total_borrowed_stable"].loc[(LP_Pool_State["LP_Pool_timestamp"]==timestamp) & (~LP_Pool_State["LP_Pool_id"].isin(pid["LP_Pool_id"]))] = LP_Pool_State["LP_Pool_id"].loc[(LP_Pool_State["LP_Pool_timestamp"]==timestamp) & (~LP_Pool_State["LP_Pool_id"].isin(pid["LP_Pool_id"]))].map(dict(borrowed.values))

    #LP_Pool_State.loc[(~LP_Pool_State["LP_Pool_id"].isin(pid["LP_Pool_id"])) and LP_Pool_State["LP_Pool_timestamp"]==timestamp,"LP_Pool_total_borrowed_stable"] = pid["LP_Pool_id"].map(dict(borrowed.values))


    return pool_util, LS_Opening, LS_Repayment, LS_Liquidation, LP_Pool_State, True

def print_report(timestamp,pool_util,withdraw_sum,liquidation_sum,repayment_sum,repayment_principal):
    print("REPORT!")
    print("Timestamp:\n", timestamp)
    print("repayment_principal:\n", repayment_principal)
    print("withdraw_sum:\n", withdraw_sum)
    print("liquidation_sum:\n", liquidation_sum)
    print("repayment_sum:\n", repayment_sum)
    print("Pool_utilisation:\n", pool_util)

    return

def calculate_borrowed_tvl(timestamp,cond,sum_borrowed,sum_deposited,repayment_sum,liquidation_sum,withdraw_sum,repayment_principal,LP_Pool_State,args):#repayment pr
    borrowed = LP_Pool_State.loc[cond, ["LP_Pool_id", "LP_Pool_total_borrowed_stable"]].reset_index(drop=True)
    deposited = LP_Pool_State.loc[cond, ["LP_Pool_id", "SYS_LP_Pool_TV_IntDep_stable"]].reset_index(drop=True)
    sum_borrowed["LP_Pool_total_borrowed_stable"] = borrowed["LP_Pool_id"].map(dict(sum_borrowed.values)) + \
                                                    borrowed["LP_Pool_total_borrowed_stable"] - borrowed["LP_Pool_id"].map(repayment_principal)
    LP_Pool_State.loc[LP_Pool_State["LP_Pool_timestamp"] == timestamp, "SYS_LS_interest"] = LP_Pool_State.loc[
                                                                                                LP_Pool_State[
                                                                                                    "LP_Pool_timestamp"] == timestamp, "LP_Pool_id"].map(
        repayment_sum) + LP_Pool_State.loc[LP_Pool_State["LP_Pool_timestamp"] == timestamp, "LP_Pool_id"].map(
        liquidation_sum)
    LP_Pool_State.loc[LP_Pool_State["LP_Pool_timestamp"] == timestamp, "SYS_LS_Pool_interest"] = LP_Pool_State.loc[
                                                                                                     LP_Pool_State[
                                                                                                         "LP_Pool_timestamp"] == timestamp, "SYS_LS_interest"] * (
                                                                                                             1 - args[
                                                                                                         "treasury_interest"] / 100)
    LP_Pool_State.loc[LP_Pool_State["LP_Pool_timestamp"] == timestamp, "SYS_TR_interest"] = LP_Pool_State.loc[
                                                                                                LP_Pool_State[
                                                                                                    "LP_Pool_timestamp"] == timestamp, "SYS_LS_interest"] * (
                                                                                                        args[
                                                                                                            "treasury_interest"] / 100)
    pool_interest_value = LP_Pool_State.loc[cond, ["LP_Pool_id", "SYS_LS_Pool_interest"]].reset_index(drop=True)

    sum_deposited["SYS_LP_Pool_TV_IntDep_stable"] = deposited["LP_Pool_id"].map(dict(sum_deposited.values)) + \
                                                    deposited["SYS_LP_Pool_TV_IntDep_stable"] + pool_interest_value[
                                                        "SYS_LS_Pool_interest"] - (
                                                        deposited["LP_Pool_id"].map(withdraw_sum))
    LP_Pool_State.loc[cond, ["LP_Pool_total_borrowed_stable"]] = LP_Pool_State.loc[cond, "LP_Pool_id"].map(
        dict(sum_borrowed.values))
    LP_Pool_State.loc[cond, ["SYS_LP_Pool_TV_IntDep_stable"]] = LP_Pool_State.loc[cond, "LP_Pool_id"].map(
        dict(sum_deposited.values))
    return borrowed,deposited,sum_borrowed,sum_deposited,LP_Pool_State


def MC_dayli_calculcations(MP_Asset,LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, SYS_LP_Withdraw,
                          LP_Pool_State,LS_Closing, PL_State,TR_Profit,TR_State,TR_Rewards_Distribution,LS_State, pool_id, args):
    repayment_sum = 0
    liquidation_sum = 0
    withdraw_sum = 0
    repayment_principal=0
    i = 0
    open_contracts = pd.DataFrame({"LS_contract_id":[]})
    PL_Interest = pd.DataFrame(
        {"PL_timestamp": [], "LP_Pool_id": [], "PL_borrowed_stable": [], "PL_deposited_stable": [], "Util": [],
         "LS_interest": [], "LP_interest": []})
    timestamps = get_timestamps(MP_Asset)
    pool_util = pd.DataFrame({"LP_Pool_id": pool_id["LP_Pool_id"], "Util": np.zeros(len(pool_id))})
    pool_interest = pd.DataFrame(
        {"LP_Pool_id": pool_id["LP_Pool_id"], "interest": np.ones(len(pool_id)) * args["base_interest"]/100})  # change from conf
    symbol_id = {v: k for k, v in dict(pool_id.values).items()}
    sum_deposited = pd.DataFrame(
        {"LP_Pool_id": pool_id["LP_Pool_id"], "SYS_LP_Pool_TV_IntDep_stable": np.zeros(len(pool_id))})
    sum_borrowed = pd.DataFrame(
        {"LP_Pool_id": pool_id["LP_Pool_id"], "LP_Pool_total_borrowed_stable": np.zeros(len(pool_id))})
    i = -1
    rewards_distributed_amnt = 0
    pool_interest_prev = pool_interest.copy()
    pool_util_prev = pool_util.copy()
    sum_borrowed_prev = sum_borrowed.copy()
    sum_deposited_prev = sum_deposited.copy()
    rewards_distributed_amnt_prev = 0
    withdraw_sum_prev = 0
    liquidation_sum_prev =0
    repayment_sum_prev = 0
    repayment_principal_prev = 0
    while i < len(timestamps)-1:
        i = i + 1
        timestamp = timestamps.values[i][0]
        if (i-1)<0:
            prev_timestamp = None
        else:
            prev_timestamp = timestamps.values[i-1][0]
        withdraw_sum, liquidation_sum, repayment_sum, repayment_principal = additions_check(withdraw_sum, liquidation_sum, repayment_sum,repayment_principal,pool_id)
        print_report(timestamp, pool_util, withdraw_sum,liquidation_sum,repayment_sum,repayment_principal)
        cond = LP_Pool_State["LP_Pool_timestamp"] == timestamp

        borrowed,deposited,sum_borrowed,sum_deposited,LP_Pool_State = calculate_borrowed_tvl(timestamp,cond, sum_borrowed, sum_deposited, repayment_sum, liquidation_sum, withdraw_sum,repayment_principal,
                               LP_Pool_State,args)
        add = LP_Pool_State.loc[LP_Pool_State["LP_Pool_timestamp"] == timestamp, ["LP_Pool_timestamp", "LP_Pool_id",
                                                                                  "LP_Pool_total_borrowed_stable",
                                                                                  "SYS_LP_Pool_TV_IntDep_stable"]]
        add = add.rename(columns={"LP_Pool_timestamp": "PL_timestamp", "LP_total_borrowed_stable": "PL_borrowed_stable",
                                  "SYS_LP_Pool_TV_IntDep_stable": "PL_deposited_stable"})
        add["Util"] = add["LP_Pool_id"].map(dict(pool_util.values))
        add["LS_interest"] = add["LP_Pool_id"].map(dict(pool_interest.values))
        LS_Opening, LS_Repayment, LS_Liquidation,LS_State,LS_Closing,open_contracts, repayment_sum, liquidation_sum,repayment_principal= LS_fill_interest(timestamp,prev_timestamp,open_contracts, pool_interest,MP_Asset, LS_Opening, LS_Repayment, LS_Liquidation,LS_State,LS_Closing, pool_id,symbol_id, args)
        # LP INTERES
        LP_Deposit, LP_Withdraw, SYS_LP_Withdraw, withdraw_sum = LP_fill_interest(timestamp, pool_interest, LP_Deposit,
                                                                                  LP_Withdraw, SYS_LP_Withdraw, pool_id,
                                                                                  args)
        add["LP_interest"] = add["LP_Pool_id"].map(dict(pool_interest.values))
        LS_Closing = LS_Closing_update(timestamp, LS_Closing, LS_Repayment, LS_Liquidation, LS_Opening)
        PL_State= PL_State_update(prev_timestamp,timestamp,PL_State,LS_Opening,LS_Repayment,LS_Liquidation,LS_Closing,LP_Deposit,LP_Withdraw,args)
        TR_Profit = TR_Profit_update(timestamp, i,rewards_distributed_amnt,nolus_price, TR_Profit, LP_Pool_State, PL_State, LS_Opening, args)
        TR_State = TR_State_update(timestamp,prev_timestamp,TR_Profit,TR_State,args)
        pool_rewards, TR_Rewards_Distribution = TR_Rewards_Distribution_update(timestamp,TR_Rewards_Distribution,TR_State,LP_Pool_State,args)
        rewards_distributed_amnt = pool_rewards["pool_rewards"].sum()
        pool_interest, pool_util = calculate_interest(timestamp, LP_Pool_State, pool_util, args)
        pool_util, LS_Opening, LS_Repayment, LS_Liquidation, LP_Pool_State, check = check_util(timestamp, pool_util,
                                                                                               LS_Opening, LS_Repayment,
                                                                                               LS_Liquidation,
                                                                                               LP_Pool_State,borrowed, args)
        if check:
            withdraw_sum = withdraw_sum_prev
            liquidation_sum = liquidation_sum_prev
            repayment_sum = repayment_sum_prev
            repayment_principal = repayment_principal_prev
            pool_interest = pool_interest_prev.copy()
            pool_util = pool_util_prev.copy()
            LP_Pool_State.loc[cond, ["SYS_LP_Pool_TV_IntDep_stable"]] = LP_Pool_State.loc[cond, "LP_Pool_id"].map(
                dict(deposited.values))
            sum_borrowed = sum_borrowed_prev.copy()
            sum_deposited = sum_deposited_prev.copy()
            rewards_distributed_amnt = rewards_distributed_amnt_prev.copy()
            i = i - 1
        else:
            withdraw_sum_prev = withdraw_sum.copy()
            liquidation_sum_prev = liquidation_sum.copy()
            repayment_sum_prev = repayment_sum.copy()
            repayment_principal_prev = repayment_principal.copy()
            pool_interest_prev = pool_interest.copy()
            pool_util_prev = pool_util.copy()
            sum_borrowed_prev = sum_borrowed.copy()
            sum_deposited_prev = sum_deposited.copy()
            rewards_distributed_amnt_prev = rewards_distributed_amnt.copy()
            PL_Interest = PL_Interest.append(add)
    # todo: TVL = SYS_LS_Pool_interest() + deposited + borrowed
    LP_Pool_State["LP_Pool_total_value_locked_stable"] = LP_Pool_State["SYS_LP_Pool_TV_IntDep_stable"]
    c = MP_Asset.loc[MP_Asset["MP_asset_symbol"].isin(args["Pool_Assets"])]
    c["LP_Pool_id"] = c["MP_asset_symbol"].map(dict(pool_id[["LP_symbol", "LP_Pool_id"]].values))
    y = c.loc[c["MP_asset_symbol"] != args["currency_stable"]]
    f = c.loc[c["MP_asset_symbol"] == args["currency_stable"], ["MP_timestamp", "MP_price_in_stable"]]
    y["stable_price"] = y["MP_timestamp"].map(dict(f.values))
    y["MP_price_in_stable"] = y["MP_price_in_stable"] / y["stable_price"]
    y = y[["MP_timestamp", "LP_Pool_id", "MP_price_in_stable"]]
    y = y.rename(columns={"MP_timestamp": "LP_Pool_timestamp"})
    LP_Pool_State = pd.merge(LP_Pool_State, y, on=["LP_Pool_timestamp", "LP_Pool_id"], how="left").fillna(1)
    LP_Pool_State["LP_Pool_total_value_locked_asset"] = LP_Pool_State["LP_Pool_total_value_locked_stable"] * \
                                                        LP_Pool_State["MP_price_in_stable"]
    LP_Pool_State = LP_Pool_State.drop("MP_price_in_stable", axis=1)

    return LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, LP_Pool_State, SYS_LP_Withdraw, PL_Interest, LS_Closing,PL_State,TR_State,TR_Profit,TR_Rewards_Distribution,LS_State

#
# def LS_int_main():
#     with open("config.json", 'r') as f:
#         args = json.load(f)
#     with open("MP_ASSET_STATE.csv", "r") as outfile:
#         MP_Asset_State = pd.read_csv(outfile, index_col=0)
#     with open("MP_ASSET.csv", "r") as outfile:
#         MP_Asset = pd.read_csv(outfile, index_col=0)
#
#     LS_Opening = pd.read_csv("LS_Opening", index_col=0)
#     LP_Deposit = pd.read_csv("LP_Deposit", index_col=0)
#     LP_Withdraw = pd.read_csv("LP_Withdraw", index_col=0)
#     SYS_LP_Withdraw = pd.read_csv("SYS_LP_Withdraw", index_col=0)
#
#     LS_Repayment = pd.read_csv("LS_Repayment", index_col=0)
#     LS_Closing = pd.read_csv("LS_Closing", index_col=0)
#     LS_Liquidation = pd.read_csv("LS_Liquidation", index_col=0)
#     pool_id = lpps.LP_pool_gen(args)
#     LP_Pool_State = pd.read_csv("LP_Pool_State.csv", index_col=0)
#
#     LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, LP_Pool_State, SYS_LP_Withdraw, PL_Interest = LP_interest_calculate(
#         LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, SYS_LP_Withdraw, LP_Pool_State, pool_id,
#         args)
#
#     PL_Interest.to_csv("PL_Interest_p2")
#     LS_Opening.to_csv("LS_Opening_p2")
#     LP_Deposit.to_csv("LP_Deposit_p2")
#     SYS_LP_Withdraw.to_csv("SYS_LP_Withdraw_p2")
#     LP_Withdraw.to_csv("LP_Withdraw_p2")
#     LS_Repayment.to_csv("LS_Repayment_p2")
#     LS_Liquidation.to_csv("LS_Liquidation_p2")
#     LP_Pool_State.to_csv("LP_Pool_State_p2")
#
#     return LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, SYS_LP_Withdraw, LP_Pool_State,PL_Interest