import json
import pandas as pd
import numpy as np
import modules.LP_Pool_State as lpps

with open("config.json", 'r') as f:
    args = json.load(f)
with open("MP_ASSET_STATE.csv", "r") as outfile:
    MP_Asset_State = pd.read_csv(outfile, index_col=0)
with open("MP_ASSET.csv", "r") as outfile:
    MP_Asset = pd.read_csv(outfile, index_col=0)

LS_Opening = pd.read_csv("LS_Opening", index_col=0)
LP_Deposit = pd.read_csv("LP_Deposit", index_col=0)
LP_Withdraw = pd.read_csv("LP_Withdraw", index_col=0)
SYS_LP_Withdraw = pd.read_csv("SYS_LP_Withdraw", index_col=0)

LS_Repayment = pd.read_csv("LS_Repayment", index_col=0)
LS_Closing = pd.read_csv("LS_Closing", index_col=0)
LS_Liquidation = pd.read_csv("LS_Liquidation", index_col=0)
pool_id = lpps.LP_pool_gen(args)
pool_id


def LP_pool_gen(args):
    list_id = ["pid" + str(sub) for sub in range(100, 100 + len(args["Pool_Assets"]))]
    #
    lpp = pd.DataFrame({"LP_Pool_id": list_id, "LP_symbol": args["Pool_Assets"]})
    return lpp


LP_Pool_State = pd.read_csv("LP_Pool_State.csv", index_col=0)


def get_timestamps(MP_Asset):
    timestamps = pd.DataFrame()
    timestamps = MP_Asset.drop_duplicates(subset=["MP_timestamp"])
    timestamps = pd.DataFrame(timestamps["MP_timestamp"])
    timestamps.rename(columns={"MP_timestamp": "PL_Interest_timestamp"})
    return timestamps


def calculate_interest(timestamp, LP_Pool_State, pool_util, args):
    # util = pd["id":Util]

    pools = LP_Pool_State.loc[LP_Pool_State["LP_Pool_timestamp"] == timestamp]
    pools = pools[["LP_Pool_id", "LP_Pool_total_borrowed_stable", "LP_Pool_total_deposited_stable"]]
    # pools = pools.rename(columns={"LP_Pool_id":"id"})
    pools = pd.merge(pools, pool_util, on="LP_Pool_id", how="left")
    util = (pools["LP_Pool_total_borrowed_stable"] / (
            pools["LP_Pool_total_deposited_stable"] + pools["LP_Pool_total_borrowed_stable"]))
    pools.loc[pools["Util"] <= args["optimal_util"] / 100, ["interest"]] = args["base_interest"] / 100 + (
            util / (args["optimal_util"] / 100)) * (args["slope1"] / 100)
    pools.loc[pools["Util"] > args["optimal_util"] / 100, ["interest"]] = args["base_interest"] / 100 + args[
        "slope1"] / 100 + (((util - args["optimal_util"] / 100) / (1 - args["optimal_util"] / 100)) * args[
        "slope2"] / 100)
    # HARDCODE CAP
    pools.loc[pools["interest"] > args["LS_interest_cap"] / 100, ["interest"]] = args["LS_interest_cap"] / 100
    pool_util["Util"] = util

    pools = pools.drop(["LP_Pool_total_borrowed_stable", "LP_Pool_total_deposited_stable", "Util"], axis=1)
    return pools, pool_util


def LS_fill_interest(timestamp, pool_interest, LS_Opening, LS_Repayment, LS_Liquidation, pool_id, args):
    # LS_Repayment
    contract_ids = LS_Opening[["LS_contract_id", "LS_loan_amnt_stable", "LP_Pool_id", "SYS_LS_expected_payment",
                               "SYS_LS_expected_penalty"]].loc[LS_Opening["LS_timestamp"] == timestamp]
    p_i = dict(pool_interest.values)
    LS_Opening["LS_interest"].loc[LS_Opening["LS_timestamp"] == timestamp] = LS_Opening["LP_Pool_id"].loc[
        LS_Opening["LS_timestamp"] == timestamp].map(p_i)
    amnt = contract_ids["LS_loan_amnt_stable"]
    payment = contract_ids["SYS_LS_expected_payment"]
    penalty = contract_ids["SYS_LS_expected_penalty"]
    pool = contract_ids["LP_Pool_id"]
    contract_ids = pd.merge(contract_ids, pool_interest, on="LP_Pool_id", how='left')
    contract_ids["repayment_amnt"] = contract_ids["LS_loan_amnt_stable"] * (
            1 + contract_ids["interest"] * (1 - contract_ids["SYS_LS_expected_penalty"] / 12)) / (
                                             contract_ids["SYS_LS_expected_payment"] + contract_ids[
                                         "SYS_LS_expected_penalty"])
    contract_ids["liquidation_amnt"] = contract_ids["LS_loan_amnt_stable"] * (
        (contract_ids["interest"] * contract_ids["SYS_LS_expected_penalty"] / 12)) / (
                                               contract_ids["SYS_LS_expected_payment"] + contract_ids[
                                           "SYS_LS_expected_penalty"])
    contract_ids = contract_ids.drop(
        ["LS_loan_amnt_stable", "LP_Pool_id", "SYS_LS_expected_payment", "SYS_LS_expected_penalty"], axis=1)
    rep = contract_ids[["LS_contract_id", "repayment_amnt"]]
    rep = rep.rename(columns={"repayment_amnt": "LS_amnt_stable"})
    rep_val = dict(rep.values)
    liq = contract_ids[["LS_contract_id", "liquidation_amnt"]]
    liq = liq.rename(columns={"liquidation_amnt": "LS_amnt_stable"})
    liq_val = dict(liq.values)
    LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(rep["LS_contract_id"]),"LS_amnt_stable"] = LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(rep["LS_contract_id"]),"LS_contract_id"].map(rep_val)
    LS_Liquidation.loc[LS_Liquidation["LS_contract_id"].isin(liq["LS_contract_id"]),"LS_amnt_stable"] = LS_Liquidation.loc[LS_Liquidation["LS_contract_id"].isin(liq["LS_contract_id"]),"LS_contract_id"].map(liq_val)


    repayment_sum = LS_Repayment.loc[LS_Repayment["LS_timestamp"] == timestamp, ["LS_symbol", "LS_amnt_stable"]]
    liquidation_sum = LS_Liquidation.loc[LS_Liquidation["LS_timestamp"] == timestamp, ["LS_symbol", "LS_amnt_stable"]]

    repayment_sum = repayment_sum.groupby("LS_symbol")["LS_amnt_stable"].sum()
    liquidation_sum = liquidation_sum.groupby("LS_symbol")["LS_amnt_stable"].sum()

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
    return LS_Opening, LS_Repayment, LS_Liquidation, repayment_sum, liquidation_sum


def fill_withdraw(timestamp, LP_Withdraw, LP_Deposit, SYS_LP_Withdraw):
    if timestamp in LP_Withdraw["LP_timestamp"].values:
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
    p_i = pool_interest
    p_i["interest"] = p_i["interest"] - args["treasury_interest"] / 100
    address_id = SYS_LP_Withdraw[["SYS_LP_contract_id"]].loc[SYS_LP_Withdraw["LP_timestamp"] == timestamp]
    address_id = LP_Deposit[["SYS_LP_contract_id", "LP_amnt_stable", "LP_Pool_id"]].loc[
        LP_Deposit["SYS_LP_contract_id"].isin(address_id["SYS_LP_contract_id"])]
    amnt = address_id[["SYS_LP_contract_id", "LP_amnt_stable"]]
    # print(amnt.loc[amnt["SYS_LP_contract_id"]=="lpaid10009081008"])
    amnt = dict(amnt.values)
    # print(amnt)
    pool = address_id["LP_Pool_id"]

    address_id = pd.merge(address_id, p_i, on="LP_Pool_id", how='left')
    p_i = dict(p_i.values)
    SYS_LP_Withdraw.loc[SYS_LP_Withdraw["LP_timestamp"] == timestamp,"LP_interest"] = SYS_LP_Withdraw["LP_Pool_id"].map(p_i)
    SYS_LP_Withdraw["LP_amnt_stable"] = SYS_LP_Withdraw["SYS_LP_contract_id"].map(amnt)
    a = SYS_LP_Withdraw.loc[SYS_LP_Withdraw["LP_timestamp"] == timestamp]
    time = SYS_LP_Withdraw.loc[
        SYS_LP_Withdraw["LP_timestamp"] == timestamp, ["LP_amnt_stable", "LP_interest", "SYS_LP_contract_id"]]
    time["LP_interest_amnt"] = time["LP_amnt_stable"] * time[
        "LP_interest"] / 365  # rework SYS_LP_Withdraw to be on dayli basis
    time = time.drop(["LP_interest", "LP_amnt_stable"], axis=1)
    time = dict(time.values)
    SYS_LP_Withdraw.loc[SYS_LP_Withdraw["LP_timestamp"] == timestamp, "LP_interest_amnt"] = \
        SYS_LP_Withdraw["SYS_LP_contract_id"].loc[SYS_LP_Withdraw["LP_timestamp"] == timestamp].map(time)

    LP_Withdraw = fill_withdraw(timestamp, LP_Withdraw, LP_Deposit, SYS_LP_Withdraw)

    withdraw_sum = LP_Withdraw.loc[LP_Withdraw["LP_timestamp"] == timestamp, ["LP_Pool_id", "LP_amnt_stable"]]
    print(withdraw_sum)
    withdraw_sum = withdraw_sum.groupby("LP_Pool_id")["LP_amnt_stable"].sum()
    if withdraw_sum.empty == True:
        withdraw_sum = pd.DataFrame({"LP_Pool_id": pool_id["LP_Pool_id"], "LS_amnt_stable": 0})
    else:
        withdraw_sum = withdraw_sum.reset_index()
        withdraw_sum = withdraw_sum.fillna(0)
    return LP_Deposit, LP_Withdraw, SYS_LP_Withdraw, withdraw_sum


def additions_check(withdraw_sum, liquidation_sum, repayment_sum):
    repayment_sum_model = pd.DataFrame({"LP_Pool_id": pool_id["LP_Pool_id"], "LS_amnt_stable": 0})
    liquidation_sum_model = pd.DataFrame({"LP_Pool_id": pool_id["LP_Pool_id"], "LS_amnt_stable": 0})
    withdraw_sum_model = pd.DataFrame({"LP_Pool_id": pool_id["LP_Pool_id"], "LP_amnt_stable": 0})

    try:
        liquidation_sum = {v: k for k, v in dict(liquidation_sum.values).items()}
        liquidation_sum_model["LS_amnt_stable"] = liquidation_sum_model["LP_Pool_id"].map(liquidation_sum).fillna(0)
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

    return withdraw_sum, liquidation_sum, repayment_sum


def LP_interest_calculate(LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, SYS_LP_Withdraw,
                          LP_Pool_State, pool_id, args):
    repayment_sum = 0
    liquidation_sum = 0
    withdraw_sum = 0
    i = 0

    PL_Interest = pd.DataFrame(
        {"PL_timestamp": [], "LP_Pool_id": [], "PL_borrowed_stable": [], "PL_deposited_stable": [], "Util": [],
         "LS_interest": [], "LP_interest": []})
    timestamps = get_timestamps(MP_Asset)
    pool_util = pd.DataFrame({"LP_Pool_id": pool_id["LP_Pool_id"], "Util": np.zeros(len(pool_id))})
    pool_interest = pd.DataFrame({"LP_Pool_id": pool_id["LP_Pool_id"], "interest": np.ones(len(pool_id))*0.1})#change from conf
    symbol_id = {v: k for k, v in dict(pool_id.values).items()}
    sum_deposited = pd.DataFrame({"LP_Pool_id": pool_id["LP_Pool_id"], "LP_Pool_total_deposited_stable": np.zeros(len(pool_id))})
    sum_borrowed = pd.DataFrame({"LP_Pool_id": pool_id["LP_Pool_id"], "LP_Pool_total_borrowed_stable": np.zeros(len(pool_id))})
    for timestamp in timestamps.values:
        timestamp = timestamp[0]
        print("REPORT!")
        print("Timestamp:\n",timestamp)
        print("Pool_utilisation:\n",pool_util)
        print("withdraw_sum:\n",withdraw_sum)
        print("liquidation_sum:\n",liquidation_sum)
        print("repayment_sum:\n",repayment_sum)
        withdraw_sum, liquidation_sum, repayment_sum = additions_check(withdraw_sum, liquidation_sum, repayment_sum)
        cond = LP_Pool_State["LP_Pool_timestamp"] == timestamp
        borrowed = LP_Pool_State.loc[cond, ["LP_Pool_id", "LP_Pool_total_borrowed_stable"]].reset_index()
        deposited = LP_Pool_State.loc[cond, ["LP_Pool_id", "LP_Pool_total_deposited_stable"]].reset_index()
        sum_borrowed["LP_Pool_total_borrowed_stable"] = borrowed["LP_Pool_id"].map(dict(sum_borrowed.values)) + borrowed["LP_Pool_total_borrowed_stable"] - (
                borrowed["LP_Pool_id"].map(repayment_sum) + borrowed["LP_Pool_id"].map(liquidation_sum))

        sum_deposited["LP_Pool_total_deposited_stable"] = deposited["LP_Pool_id"].map(dict(sum_deposited.values)) + deposited["LP_Pool_total_deposited_stable"] - (
            deposited["LP_Pool_id"].map(withdraw_sum))
        LP_Pool_State.loc[cond, ["LP_Pool_total_borrowed_stable"]] =  LP_Pool_State.loc[cond, "LP_Pool_id"].map(dict(sum_borrowed.values))
        LP_Pool_State.loc[cond, ["LP_Pool_total_deposited_stable"]] = LP_Pool_State.loc[cond, "LP_Pool_id"].map(dict(sum_deposited.values))

        add = LP_Pool_State.loc[LP_Pool_State["LP_Pool_timestamp"] == timestamp, ["LP_Pool_timestamp", "LP_Pool_id",
                                                                                  "LP_Pool_total_borrowed_stable",
                                                                                  "LP_Pool_total_deposited_stable"]]
        add = add.rename(columns={"LP_Pool_timestamp": "PL_timestamp", "LP_total_borrowed_stable": "PL_borrowed_stable",
                                  "LP_Pool_total_deposited_stable": "PL_deposited_stable"})
        add["Util"] = add["LP_Pool_id"].map(dict(pool_util.values))
        add["LS_interest"] = add["LP_Pool_id"].map(dict(pool_interest.values))

        LS_Opening, LS_Repayment, LS_Liquidation, repayment_sum, liquidation_sum = LS_fill_interest(timestamp,
                                                                                                    pool_interest,
                                                                                                    LS_Opening,
                                                                                                    LS_Repayment,
                                                                                                    LS_Liquidation,
                                                                                                    pool_id, args)
        repayment_sum["LP_Pool_id"] = repayment_sum["LS_symbol"].map(symbol_id)
        repayment_sum = repayment_sum.drop("LS_symbol", axis=1)
        liquidation_sum["LP_Pool_id"] = liquidation_sum["LS_symbol"].map(symbol_id)
        liquidation_sum = liquidation_sum.drop("LS_symbol", axis=1)
        # LP INTERES
        LP_Deposit, LP_Withdraw, SYS_LP_Withdraw, withdraw_sum = LP_fill_interest(timestamp, pool_interest, LP_Deposit,
                                                                                  LP_Withdraw, SYS_LP_Withdraw, pool_id,
                                                                                  args)
        add["LP_interest"] = add["LP_Pool_id"].map(dict(pool_interest.values))
        PL_Interest = PL_Interest.append(add)
        pool_interest, pool_util = calculate_interest(timestamp, LP_Pool_State, pool_util, args)

    return LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, LP_Pool_State, SYS_LP_Withdraw, PL_Interest


def LS_int_main():
    with open("config.json", 'r') as f:
        args = json.load(f)
    with open("MP_ASSET_STATE.csv", "r") as outfile:
        MP_Asset_State = pd.read_csv(outfile, index_col=0)
    with open("MP_ASSET.csv", "r") as outfile:
        MP_Asset = pd.read_csv(outfile, index_col=0)

    LS_Opening = pd.read_csv("LS_Opening", index_col=0)
    LP_Deposit = pd.read_csv("LP_Deposit", index_col=0)
    LP_Withdraw = pd.read_csv("LP_Withdraw", index_col=0)
    SYS_LP_Withdraw = pd.read_csv("SYS_LP_Withdraw", index_col=0)

    LS_Repayment = pd.read_csv("LS_Repayment", index_col=0)
    LS_Closing = pd.read_csv("LS_Closing", index_col=0)
    LS_Liquidation = pd.read_csv("LS_Liquidation", index_col=0)
    pool_id = lpps.LP_pool_gen(args)
    LP_Pool_State = pd.read_csv("LP_Pool_State.csv", index_col=0)

    LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, LP_Pool_State, SYS_LP_Withdraw = LP_interest_calculate(
        LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, SYS_LP_Withdraw, LP_Pool_State, pool_id,
        args)
    return LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, SYS_LP_Withdraw, LP_Pool_State
