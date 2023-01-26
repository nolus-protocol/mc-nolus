import pandas as pd
import numpy as np
import warnings
from modules.PL_CurrentState import PL_State_update
from modules.TR_Profit import TR_Profit_update
from modules.TR_Rewards_Distribution import TR_Rewards_Distribution_update
from modules.TR_State import TR_State_update
from modules.LS_Closing import LS_Closing_update,LS_Closing_market_cond_update
from modules.LP_Pool_State import LP_Pool_State_update
import modules.LS_State as LS_State
import math

warnings.filterwarnings('ignore')


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


def check_util(timestamp, pool_util, LS_Opening, LS_Repayment, LS_Liquidation, LP_Pool_State, args):
    pid = pool_util.loc[pool_util["Util"] >= args["max_pool_util"] / 100]
    if pid.empty:
        return pool_util, LS_Opening, LS_Repayment, LS_Liquidation, LP_Pool_State, False

    contract_ids = LS_Opening[["LS_contract_id", "LP_Pool_id"]].loc[LS_Opening["LS_timestamp"] == timestamp]
    contract_ids = contract_ids.loc[contract_ids["LP_Pool_id"].isin(pid["LP_Pool_id"])]

    LS_Opening = LS_Opening.loc[~LS_Opening["LS_contract_id"].isin(contract_ids["LS_contract_id"])]
    LS_Repayment = LS_Repayment.loc[~LS_Repayment["LS_contract_id"].isin(contract_ids["LS_contract_id"])]
    LS_Liquidation = LS_Liquidation.loc[~LS_Liquidation["LS_contract_id"].isin(contract_ids["LS_contract_id"])]
    #LP_Pool_State["LP_Pool_total_borrowed_stable"].loc[(LP_Pool_State["LP_Pool_timestamp"]==timestamp) & (LP_Pool_State["LP_Pool_id"].isin(pid["LP_Pool_id"]))] = 0
    #LP_Pool_State["LP_Pool_total_borrowed_stable"].loc[(LP_Pool_State["LP_Pool_timestamp"]==timestamp) & (~LP_Pool_State["LP_Pool_id"].isin(pid["LP_Pool_id"]))] = LP_Pool_State["LP_Pool_id"].loc[(LP_Pool_State["LP_Pool_timestamp"]==timestamp) & (~LP_Pool_State["LP_Pool_id"].isin(pid["LP_Pool_id"]))].map(dict(borrowed.values))

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


def ls_open_contracts(LS_Opening,LS_Repayment,LS_Liquidation,timestamp,pool_interest,args):
    #pool_int - int_min
    interest_int_min = pd.DataFrame(
        {"LP_Pool_id": pool_interest["LP_Pool_id"], "interest": np.ones(len(pool_interest)) * args["base_interest"]/100})

    interest_int_min["interest"] = pool_interest["interest"] - args["base_interest"]
    #int_max - int_min
    int_min_max = pd.DataFrame(
        {"LP_Pool_id": pool_interest["LP_Pool_id"], "interest": np.ones(len(pool_interest)) * args["base_interest"]/100})
    int_min_max["interest"] = args["LS_interest_cap"] - int_min_max["interest"]
    dem = pd.DataFrame(
        {"LP_Pool_id": pool_interest["LP_Pool_id"], "demand": np.ones(len(pool_interest))})
    dem["demand"] = (((np.exp(interest_int_min["interest"]) - np.exp(int_min_max["interest"]))/(1-np.exp(int_min_max["interest"])))*(args["LS_demand_prc_int_min"]-args["LS_demand_prc_int_max"]))+args["LS_demand_prc_int_max"]

    contracts = LS_Opening.loc[LS_Opening["LS_timestamp"]==timestamp,["LP_Pool_id"]]
    index = list(contracts.index)
    contracts["index"] = index
    count = contracts.groupby("LP_Pool_id").count().reset_index()
    count["demand"] = count["LP_Pool_id"].map(dict(dem.values))/100
    count["count"] = count["index"]*count["demand"].astype(int)
    count["drop"] = count["index"] - count["count"]
    contracts = contracts.groupby("LP_Pool_id").agg(list).reset_index()
    idxdrop = []
    for pool in contracts["LP_Pool_id"]:
        np.random.seed(args["seed"])
        try:
            idlist = list(contracts.loc[contracts["LP_Pool_id"] == pool]["index"].explode())
            i = int(count.loc[count["LP_Pool_id"] == pool]["drop"])
            idxdrop = np.random.choice(idlist,size=i).tolist()
            cid_drop = LS_Opening.loc[idxdrop]["LS_contract_id"]
            if len(idxdrop) != 0:
                LS_Opening = LS_Opening.drop(idxdrop, axis=0)
                LS_Repayment = LS_Repayment.drop(LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(cid_drop)].index,axis=0)
                LS_Liquidation = LS_Liquidation.drop(LS_Liquidation.loc[LS_Liquidation["LS_contract_id"].isin(cid_drop)].index,axis=0)
        except:
            pass
        args["seed"] +=1

    #ls opening .loc timestamp . groupby(LP_Pool_id)
    #ls opening .drop(random_choise(count-count_ini, count_ini.index))
    # ret
    return LS_Opening,LS_Repayment,LS_Liquidation

def nolus_price_adjustment(timestamp,pool_util,pool_util_prev,nolus_price,TR_Profit,args):
    args["platform_nolus_token_count"] = args["platform_nolus_token_count"] + TR_Profit.loc[TR_Profit["TR_Profit_timestamp"]==timestamp,"TR_Profit_amnt_nls"].values
    #change val with platform token count
    val = TR_Profit.loc[TR_Profit["TR_Profit_timestamp"]==timestamp,"TR_Profit_amnt_nls"].values
    if val < 0:
        return nolus_price
    proportion = val/args["nls_all_tokens"]
    if len(proportion) == 0:
        proportion = 0
    pu = pool_util.copy()
    pup = pool_util_prev.copy()
    util = 1 - pu["Util"].mean()/pup["Util"].mean()
    if pu["Util"].mean()/pup["Util"].mean() > 1:
        util = 0.12
    if math.isnan(util) or math.isinf(util):
        util = 0
    price = nolus_price.loc[nolus_price["MP_timestamp"]==timestamp,["MP_price_in_stable"]]
    p1 = price + price*util
    # -0.5*cena1 + (1 - (-0.5) * cena) 1.5 * x -  0.5*f(x)
    price_new = proportion*p1 + (1-proportion)*price
    price = price_new - price
    nolus_price.loc[nolus_price["MP_timestamp"] > timestamp, "add"] = price.values
    nolus_price.loc[nolus_price["MP_timestamp"] <= timestamp, "add"] = 0
    nolus_price["MP_price_in_stable"] = nolus_price["MP_price_in_stable"] + nolus_price["add"]
    nolus_price = nolus_price.drop("add",axis=1)
    return nolus_price

def rep_records(timestamp,LS_Opening,LS_Repayment,LS_Liquidation):
    contract_ids = LS_Opening.loc[LS_Opening["LS_timestamp"]==timestamp]
    contract_ids["repayment_amnt"] = (contract_ids["LS_loan_amnt_stable"] + contract_ids["LS_loan_amnt_stable"] * (
                contract_ids["LS_interest"] / 12) * contract_ids["SYS_LS_expected_payment"]) / (
                                             contract_ids["SYS_LS_expected_payment"] + contract_ids[
                                         "SYS_LS_expected_penalty"])

    contract_ids["liquidation_amnt"] = (contract_ids["LS_loan_amnt_stable"] *
                                        (contract_ids["LS_interest"] / 12) * contract_ids["SYS_LS_expected_penalty"]) / (
                                               contract_ids["SYS_LS_expected_payment"] + contract_ids[
                                           "SYS_LS_expected_penalty"])
    contract_ids["ls_principal_stable"] = contract_ids["LS_loan_amnt_stable"] / (
                contract_ids["SYS_LS_expected_payment"] + contract_ids[
            "SYS_LS_expected_penalty"])
    contract_ids["ls_rep_margins"] = (contract_ids["repayment_amnt"] - contract_ids["ls_principal_stable"]) / 4
    contract_ids["ls_liq_margins"] = contract_ids["liquidation_amnt"] / 4

    LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(contract_ids["LS_contract_id"]),"LS_amnt_stable"] = LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(contract_ids["LS_contract_id"]),"LS_contract_id"].map(dict(contract_ids[["LS_contract_id","repayment_amnt"]].values))
    LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(contract_ids["LS_contract_id"]),"LS_principal_stable"] = LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(contract_ids["LS_contract_id"]),"LS_contract_id"].map(dict(contract_ids[["LS_contract_id","ls_principal_stable"]].values))
    LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(contract_ids["LS_contract_id"]),["LS_current_margin_stable","LS_current_interest_stable","LS_prev_margin_stable","LS_prev_interest_stable"]] = LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(contract_ids["LS_contract_id"]),"LS_contract_id"].map(dict(contract_ids[["LS_contract_id","ls_rep_margins"]].values))

    LS_Liquidation.loc[LS_Liquidation["LS_contract_id"].isin(contract_ids["LS_contract_id"]),"LS_amnt_stable"] = LS_Liquidation.loc[LS_Liquidation["LS_contract_id"].isin(contract_ids["LS_contract_id"]),"LS_contract_id"].map(dict(contract_ids[["LS_contract_id","liquidation_amnt"]].values))
    LS_Liquidation.loc[LS_Liquidation["LS_contract_id"].isin(contract_ids["LS_contract_id"]),["LS_current_margin_stable","LS_current_interest_stable","LS_prev_margin_stable","LS_prev_interest_stable"]] = LS_Liquidation.loc[LS_Liquidation["LS_contract_id"].isin(contract_ids["LS_contract_id"]),"LS_contract_id"].map(dict(contract_ids[["LS_contract_id","ls_liq_margins"]].values))

    return LS_Repayment,LS_Liquidation


def new_contr(timestamp,LS_Opening,LS_Repayment,open_contracts):

    temp = LS_Opening.loc[LS_Opening["LS_timestamp"] == timestamp].copy()
    rep_temp = LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(temp["LS_contract_id"])].copy()
    rep_temp = rep_temp.drop_duplicates(subset="LS_contract_id")
    open_contracts["LS_timestamp"] = timestamp
    contract_ids = pd.DataFrame({"LS_timestamp":temp["LS_timestamp"].values,"LS_contract_id": temp["LS_contract_id"].values,"LP_Pool_id":temp["LP_Pool_id"].values,
                                 "LS_amnt_stable":temp["LS_loan_amnt_stable"].values,"LS_asset_symbol":temp["LS_asset_symbol"].values,
                                 "LS_amnt_asset":(temp["LS_loan_amnt_asset"].values+temp["LS_cltr_amnt_asset"].values),
                                 "SYS_asset_price_stable":0,"LS_principal_stable":temp["LS_loan_amnt_stable"].values,
                                 "LS_current_margin_stable":rep_temp["LS_current_margin_stable"].values,
                                 "LS_prev_margin_stable":rep_temp["LS_prev_margin_stable"].values,
                                 "LS_current_interest_stable":rep_temp["LS_current_interest_stable"].values,
                                 "LS_prev_interest_stable":rep_temp["LS_prev_interest_stable"].values,
                                 "SYS_liability_LPN":0, "LS_loan_amnt":(temp["LS_loan_amnt_asset"].values+temp["LS_cltr_amnt_asset"].values),"SYS_lease_amnt_LPN":0})

    open_contracts = pd.concat([open_contracts,contract_ids],axis=0)
    return open_contracts


def calculate_liability(timestamp,open_ls_contracts, MP_Asset):
    timest_price = MP_Asset.loc[MP_Asset["MP_timestamp"]==timestamp]
    open_ls_contracts["LS_amnt_stable"] = open_ls_contracts["LS_principal_stable"] + 4*open_ls_contracts["LS_current_margin_stable"]
    open_ls_contracts["SYS_liability_LPN"] = open_ls_contracts["LS_principal_stable"] + 4*open_ls_contracts["LS_current_margin_stable"]
    open_ls_contracts["SYS_asset_price_stable"] = open_ls_contracts["LS_asset_symbol"].map(dict(timest_price[["MP_asset_symbol","MP_price_in_stable"]].values))
    open_ls_contracts["SYS_lease_amnt_LPN"] = open_ls_contracts["LS_loan_amnt"]*open_ls_contracts["SYS_asset_price_stable"]
    return open_ls_contracts


def ls_contract_manager(timestamp, open_ls_contracts, LS_Opening, LS_Repayment, LS_Liquidation,MP_Asset, pool_interest,args):
    #set_interest
    p_i = dict(pool_interest.values)
    LS_Opening["LS_interest"].loc[LS_Opening["LS_timestamp"] == timestamp] = LS_Opening["LP_Pool_id"].loc[
        LS_Opening["LS_timestamp"] == timestamp].map(p_i)
    # create type 1 repayment records
    LS_Repayment,LS_Liquidation = rep_records(timestamp,LS_Opening,LS_Repayment,LS_Liquidation)
    # create open ls record
    open_ls_contracts = new_contr(timestamp,LS_Opening,LS_Repayment,open_ls_contracts)
    #calculate dayli operatable variables : liabilityLPN,SYS_lease_amnt_LPN,SYS_asset_price_stable
    open_ls_contracts = calculate_liability(timestamp,open_ls_contracts,MP_Asset)
    open_ls_contracts=open_ls_contracts.reset_index(drop=True)
    return open_ls_contracts, LS_Repayment, LS_Liquidation


def lp_record_creator(timestamp, open_lp_contracts, LP_Deposit, p_i):

    dep_t = LP_Deposit.loc[LP_Deposit["LP_timestamp"]==timestamp].copy()
    dep_t["interest"] = dep_t["LP_Pool_id"].map(dict(p_i.values))
    temp = pd.DataFrame({'LP_timestamp':dep_t["LP_timestamp"], 'LP_address_id':dep_t["LP_address_id"],
                         'SYS_LP_contract_id':dep_t["SYS_LP_contract_id"], 'LP_Pool_id':dep_t["LP_Pool_id"],
       'LP_withdraw_height':0, 'LP_withdraw_idx':0, 'LP_amnt_stable':dep_t["LP_amnt_stable"],
       'LP_interest':dep_t["interest"], 'LP_interest_amnt':0,"SYS_LP_interest_gain":0,"LP_Lender_rewards_nls_total":0,"LP_Lender_rewards_nls_current":0,"LP_Lender_rewards_stable":0})
    open_lp_contracts = pd.concat([open_lp_contracts,temp],axis=0)
    open_lp_contracts["LP_interest_amnt"] = open_lp_contracts["LP_amnt_stable"]*(open_lp_contracts["LP_interest"]/365)
    open_lp_contracts["SYS_LP_interest_gain"] = open_lp_contracts["SYS_LP_interest_gain"] + open_lp_contracts["LP_interest_amnt"]
    open_lp_contracts = open_lp_contracts.reset_index(drop=True)
    return  open_lp_contracts

def lp_contract_manager(timestamp, open_lp_contracts, LP_Deposit, SYS_LP_Withdraw, pool_interest,args):
    #define interest
    p_i = pool_interest
    p_i["interest"] = p_i["interest"] - args["treasury_interest"] / 100
    #open lp_contracts calculate interest for current date
    open_lp_contracts = lp_record_creator(timestamp,open_lp_contracts,LP_Deposit,p_i)

    #SYS_LP_Withdraw = pd.concat([SYS_LP_Withdraw,open_lp_contracts],axis=0)
    return open_lp_contracts,SYS_LP_Withdraw


def ls_payment_events(timestamp,open_ls_contracts, rep, liq,MP_Asset,symbol_id,args):
    #apply repayments
    liq = liq.loc[liq["LS_transaction_type"]==1]
    rep_cond = open_ls_contracts["LS_contract_id"].isin(rep["LS_contract_id"])
    liq_cond = open_ls_contracts["LS_contract_id"].isin(liq["LS_contract_id"])
    open_ls_contracts.loc[rep_cond,"LS_amnt_stable"] = open_ls_contracts.loc[rep_cond,"LS_amnt_stable"] - open_ls_contracts.loc[rep_cond,"LS_contract_id"].map(dict(rep[["LS_contract_id","LS_amnt_stable"]].values))
    open_ls_contracts.loc[rep_cond,"LS_principal_stable"] = open_ls_contracts.loc[rep_cond,"LS_principal_stable"] - open_ls_contracts.loc[rep_cond,"LS_contract_id"].map(dict(rep[["LS_contract_id","LS_principal_stable"]].values))

    repayment_sum = rep[["LS_symbol","LS_amnt_stable"]].groupby("LS_symbol")["LS_amnt_stable"].sum()
    repayment_principal = rep[["LS_symbol","LS_principal_stable"]].groupby("LS_symbol")["LS_principal_stable"].sum()
    #apply liquidation type 1 alll repayments = nan
    if timestamp == "2021-05-28":
        print(0)
    open_ls_contracts["substract"] = open_ls_contracts["LS_contract_id"].map(dict(liq[["LS_contract_id","LS_amnt_stable"]].values))
    open_ls_contracts.loc[liq_cond,"substract"] = open_ls_contracts.loc[liq_cond,"substract"]/open_ls_contracts.loc[liq_cond,"SYS_asset_price_stable"]
    open_ls_contracts.loc[liq_cond,"LS_loan_amnt"] =open_ls_contracts.loc[liq_cond,"LS_loan_amnt"] -open_ls_contracts["substract"]
    open_ls_contracts = open_ls_contracts.drop(["substract"],axis=1)
    liquidation_sum = liq[["LS_symbol","LS_amnt_stable"]].groupby("LS_symbol")["LS_amnt_stable"].sum()
    liquidation_sum, repayment_sum = check_payments(args, liquidation_sum, repayment_sum, symbol_id)
    #recalculate liability
    open_ls_contracts = calculate_liability(timestamp,open_ls_contracts,MP_Asset)
    return open_ls_contracts, repayment_sum,repayment_principal,liquidation_sum


def check_payments(args, liquidation_sum, repayment_sum, symbol_id):
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
    repayment_sum["LP_Pool_id"] = repayment_sum["LS_symbol"].map(symbol_id)
    repayment_sum = repayment_sum.drop("LS_symbol", axis=1)
    liquidation_sum["LP_Pool_id"] = liquidation_sum["LS_symbol"].map(symbol_id)
    liquidation_sum = liquidation_sum.drop("LS_symbol", axis=1)
    return liquidation_sum, repayment_sum


def liq_type2_payments_update(timestamp, contracts_to_liquidate, LS_Repayment, LS_Liquidation):
    #UPDATE REPAYMENT AND LIQUIDATION RECORDS AFTER TYPE 2 LIQUIDATION
    rep = LS_Repayment.loc[LS_Repayment["LS_timestamp"].astype("str")>=timestamp]
    liq = LS_Liquidation.loc[LS_Liquidation["LS_timestamp"].astype("str")>=timestamp]
    rep = rep.loc[rep["LS_contract_id"].isin(contracts_to_liquidate["LS_contract_id"])]
    liq = liq.loc[liq["LS_contract_id"].isin(contracts_to_liquidate["LS_contract_id"])]
    liq = liq.loc[liq["LS_transaction_type"]<2]

    rep["post_to_pre"] = rep["LS_contract_id"].map(dict(contracts_to_liquidate[["LS_contract_id","post_to_pre"]].values))
    liq["post_to_pre"] =liq["LS_contract_id"].map(dict(contracts_to_liquidate[["LS_contract_id","post_to_pre"]].values))

    rep["LS_principal_stable"] = rep["LS_principal_stable"]*rep["post_to_pre"]
    rep["LS_current_margin_stable"] =rep["LS_current_margin_stable"]*rep["post_to_pre"]
    rep["LS_current_interest_stable"] = rep["LS_current_margin_stable"]
    rep["LS_prev_margin_stable"]= rep["LS_current_margin_stable"]
    rep["LS_prev_interest_stable"] = rep["LS_current_margin_stable"]
    rep["LS_amnt_stable"] = rep["LS_principal_stable"] + 4*rep["LS_current_margin_stable"]

    liq["LS_current_margin_stable"] = liq["LS_current_margin_stable"] * liq["post_to_pre"]
    liq["LS_current_interest_stable"] = liq["LS_current_margin_stable"]
    liq["LS_prev_margin_stable"] = liq["LS_current_margin_stable"]
    liq["LS_prev_interest_stable"] = liq["LS_current_margin_stable"]
    liq["LS_amnt_stable"] = 4*liq["LS_current_margin_stable"]
    rep = rep.drop("post_to_pre",axis=1)
    liq = liq.drop("post_to_pre",axis=1)
    rep_drop_cond = ((LS_Repayment["LS_contract_id"].isin(contracts_to_liquidate["LS_contract_id"])) & (LS_Repayment["LS_timestamp"].astype("str") >= timestamp))
    liq_drop_cond = ((LS_Liquidation["LS_contract_id"].isin(contracts_to_liquidate["LS_contract_id"])) & (LS_Liquidation["LS_timestamp"].astype("str") >= timestamp) & (LS_Liquidation["LS_transaction_type"] < 2))
    LS_Repayment.drop(rep_drop_cond.index,axis=0)
    LS_Liquidation.drop(liq_drop_cond.index,axis=0)
    LS_Repayment = pd.concat([LS_Repayment,rep],axis=0)
    LS_Liquidation = pd.concat([LS_Liquidation,liq],axis=0)

    return LS_Repayment,LS_Liquidation


def update_current_state(timestamp,contracts_to_liquidate,LS_Repayment,LS_Liquidation):
    contracts_to_liquidate["LS_amnt_stable"] = contracts_to_liquidate["LS_amnt_stable"] - contracts_to_liquidate["liq_amnt"]
    contracts_to_liquidate["SYS_lease_amnt_LPN"] = contracts_to_liquidate["SYS_lease_amnt_LPN"] - contracts_to_liquidate["liq_amnt"]
    contracts_to_liquidate["LS_loan_amnt"] = contracts_to_liquidate["SYS_lease_amnt_LPN"]/contracts_to_liquidate["SYS_asset_price_stable"]
    contracts_to_liquidate["LS_amnt_asset"] = contracts_to_liquidate["LS_loan_amnt"]
    contracts_to_liquidate["post_to_pre"] = (contracts_to_liquidate["LS_amnt_stable"] - 4*contracts_to_liquidate["LS_current_margin_stable"])/contracts_to_liquidate["LS_principal_stable"]
    contracts_to_liquidate["LS_principal_stable"] = (contracts_to_liquidate["LS_amnt_stable"] - 4*contracts_to_liquidate["LS_current_margin_stable"])
    LS_Repayment,LS_Liquidation = liq_type2_payments_update(timestamp,contracts_to_liquidate,LS_Repayment,LS_Liquidation)
    contracts_to_liquidate["SYS_liability_LPN"] = contracts_to_liquidate["LS_amnt_stable"]

    return contracts_to_liquidate,LS_Repayment,LS_Liquidation


def liq_type2_handler(timestamp,open_ls_contracts, LS_Repayment, LS_Liquidation, LS_Closing,pool_id, args):
    liq_condition = (open_ls_contracts["SYS_lease_amnt_LPN"]*args["max_cltr_percent"]/100)<(open_ls_contracts["SYS_liability_LPN"])
    contracts_to_liquidate = open_ls_contracts.loc[liq_condition]
    if contracts_to_liquidate.empty:
        liquidation_sum = LS_Liquidation.loc[LS_Liquidation["LS_timestamp"] == timestamp]
        liquidation_type_2 = liquidation_sum.loc[liquidation_sum["LS_transaction_type"] >= 2]
        liquidation_type_2 = liquidation_type_2.groupby("LS_symbol")["LS_amnt_stable"].sum()
        return  open_ls_contracts,liquidation_type_2,LS_Closing,LS_Repayment,LS_Liquidation
    #calculate liquidation amnt
    contracts_to_liquidate["liq_amnt"] = (contracts_to_liquidate["SYS_liability_LPN"] - (
                (args["healthy_cltr_percent"] / 100) * contracts_to_liquidate["SYS_lease_amnt_LPN"])) / (
                                                     1 - (args["healthy_cltr_percent"] / 100))
    contracts_to_liquidate["liq_amnt"] = contracts_to_liquidate[["SYS_lease_amnt_LPN", "liq_amnt"]].min(axis=1)
    #apply liquidation amnt to current_state
    contracts_to_close = contracts_to_liquidate.loc[contracts_to_liquidate["liq_amnt"] == contracts_to_liquidate["SYS_lease_amnt_LPN"]]
    contracts_to_liquidate = contracts_to_liquidate.loc[~contracts_to_liquidate["LS_contract_id"].isin(contracts_to_close["LS_contract_id"])]
    #contracts_to_liquidate,LS_Repayment,LS_Liquidation = update_current_state(timestamp,contracts_to_liquidate,LS_Repayment,LS_Liquidation)
    #create liquidation record
    LS_Liquidation = LS_State.liq_record_creation(timestamp,contracts_to_liquidate,LS_Liquidation,pool_id)
    LS_Liquidation = LS_State.liq_record_creation(timestamp,contracts_to_close,LS_Liquidation,pool_id)
    LS_Liquidation.loc[(LS_Liquidation["LS_timestamp"]==timestamp) & (LS_Liquidation["LS_contract_id"].isin(contracts_to_close["LS_contract_id"])),"LS_transaction_type"]=2
    LS_Closing = LS_Closing_market_cond_update(timestamp, LS_Closing, LS_Liquidation)
    #drop records for closed contracts and partialli liquidated
    open_ls_contracts = open_ls_contracts.drop(open_ls_contracts.loc[open_ls_contracts["LS_contract_id"].isin(contracts_to_close["LS_contract_id"])].index,axis=0)
    open_ls_contracts = open_ls_contracts.drop(open_ls_contracts.loc[open_ls_contracts["LS_contract_id"].isin(contracts_to_liquidate["LS_contract_id"])].index,axis=0)
    #append partially liquidated records to open contracts
    contracts_to_liquidate,LS_Repayment,LS_Liquidation = update_current_state(timestamp,contracts_to_liquidate,LS_Repayment,LS_Liquidation)
    contracts_to_liquidate = contracts_to_liquidate.drop("liq_amnt",axis=1)
    open_ls_contracts = pd.concat([open_ls_contracts,contracts_to_liquidate],axis=0)
    #take liquidation sum
    liquidation_sum = LS_Liquidation.loc[LS_Liquidation["LS_timestamp"] == timestamp]
    liquidation_type_2 = liquidation_sum.loc[liquidation_sum["LS_transaction_type"] >= 2]
    liquidation_type_2 = liquidation_type_2.groupby("LS_symbol")["LS_amnt_stable"].sum().reset_index()
    return open_ls_contracts,liquidation_type_2,LS_Closing,LS_Repayment,LS_Liquidation


def ls_event_manager(timestamp, open_ls_contracts, LS_Repayment, LS_Liquidation, LS_Closing,MP_Asset,symbol_id,pool_id,args):
    #detect repayments and liquidations
    rep = LS_Repayment.loc[LS_Repayment["LS_timestamp"].astype("str")==timestamp]
    liq = LS_Liquidation.loc[LS_Liquidation["LS_timestamp"].astype("str")==timestamp]
    # apply repayments and liquidations
    open_ls_contracts, repayment_sum,repayment_principal,liquidation_type_1 = ls_payment_events(timestamp,open_ls_contracts, rep, liq,MP_Asset,symbol_id,args)
    #check liquidation type 2
    open_ls_contracts,liquidation_type_2,LS_Closing,LS_Repayment,LS_Liquidation = liq_type2_handler(timestamp,open_ls_contracts,LS_Repayment,LS_Liquidation,LS_Closing,pool_id,args)
    if liquidation_type_1.empty == True:
        liquidation_type_1 = pd.DataFrame({"LS_symbol": args["Pool_Assets"], "LS_amnt_stable": 0})
    else:
        liquidation_type_1 = liquidation_type_1.reset_index(drop=True)
        liquidation_type_1 = liquidation_type_1.fillna(0)
    if liquidation_type_2.empty == True:
        liquidation_type_2 = pd.DataFrame({"LS_symbol": args["Pool_Assets"], "LS_amnt_stable": 0})
    else:
        liquidation_type_2 = liquidation_type_2.reset_index(drop=True)
        liquidation_type_2 = liquidation_type_2.fillna(0)
    if repayment_sum.empty == True:
        repayment_sum = pd.DataFrame({"LS_symbol": args["Pool_Assets"], "LS_amnt_stable": 0})
    else:
        repayment_sum = repayment_sum.reset_index(drop=True)
        repayment_sum = repayment_sum.fillna(0)
    if repayment_principal.empty == True:
        repayment_principal = pd.DataFrame({"LS_symbol": args["Pool_Assets"], "LS_principal_stable": 0})
    else:
        repayment_principal = repayment_principal.reset_index()
        repayment_principal = repayment_principal.fillna(0)
    liquidation_sum = pd.DataFrame({"LS_symbol": args["Pool_Assets"], "LS_amnt_stable": 0})
    liquidation_sum["LS_amnt_stable"] = liquidation_sum["LS_amnt_stable"] + liquidation_sum["LS_symbol"].map(dict(liquidation_type_2[["LS_symbol","LS_amnt_stable"]].values))

    try:
        repayment_sum["LP_Pool_id"] = repayment_sum["LS_symbol"].map(symbol_id)
        repayment_sum = repayment_sum.drop("LS_symbol", axis=1)
    except:
        pass
    liquidation_sum["LP_Pool_id"] = liquidation_sum["LS_symbol"].map(symbol_id)
    liquidation_sum = liquidation_sum.drop("LS_symbol", axis=1)
    liquidation_sum["LS_amnt_stable"] =liquidation_sum["LS_amnt_stable"] + liquidation_sum["LP_Pool_id"].map(dict(liquidation_type_1[["LP_Pool_id","LS_amnt_stable"]].values))
    repayment_principal["LP_Pool_id"] = repayment_principal["LS_symbol"].map(symbol_id)
    repayment_principal = repayment_principal.drop("LS_symbol",axis=1)
    return open_ls_contracts,repayment_sum,repayment_principal,liquidation_sum,LS_Liquidation,LS_Closing


def lp_event_manager(timestamp, open_lp_contracts, LP_Withdraw, SYS_LP_Withdraw,pool_id):
    contracts_to_close = LP_Withdraw.loc[LP_Withdraw["LP_timestamp"].astype("str")==timestamp]
    #take withdraw sum
    if contracts_to_close.empty:
        withdraw_sum = pd.DataFrame({"LP_Pool_id": pool_id["LP_Pool_id"], "LP_amnt_stable": 0})
        return withdraw_sum,open_lp_contracts,LP_Withdraw,SYS_LP_Withdraw
    withdraw = open_lp_contracts.loc[open_lp_contracts["SYS_LP_contract_id"].isin(contracts_to_close["SYS_LP_contract_id"]),["SYS_LP_contract_id","LP_Pool_id","LP_amnt_stable","SYS_LP_interest_gain"]]
    withdraw["sum"] = withdraw["LP_amnt_stable"] + withdraw["SYS_LP_interest_gain"]
    LP_Withdraw.loc[LP_Withdraw["SYS_LP_contract_id"].isin(withdraw["SYS_LP_contract_id"]),"LP_amnt_stable"] = LP_Withdraw.loc[LP_Withdraw["SYS_LP_contract_id"].isin(withdraw["SYS_LP_contract_id"]),"SYS_LP_contract_id"].map(dict(withdraw[["SYS_LP_contract_id","sum"]]))
    #drop lp contracts
    open_lp_contracts = open_lp_contracts.loc[~open_lp_contracts["SYS_LP_contract_id"].isin(withdraw["SYS_LP_contract_id"])]
    #normalize withdraw_sum
    withdraw_sum = withdraw[["LP_Pool_id","sum"]].groupby("LP_Pool_id").sum().reset_index()
    withdraw_sum = withdraw_sum.rename(columns={"sum":"LP_amnt_stable"})
    return withdraw_sum,open_lp_contracts,LP_Withdraw,SYS_LP_Withdraw


def close_contracts(timestamp,open_ls_contracts, LS_Closing, LS_Repayment):
    close_cond = LS_Repayment.loc[LS_Repayment["LS_timestamp"].astype("str")>timestamp].copy()
    close_cond = open_ls_contracts.loc[~open_ls_contracts["LS_contract_id"].isin(close_cond["LS_contract_id"])]
    closing_record = pd.DataFrame({"LS_timestamp":np.repeat(timestamp,len(close_cond)),"LS_contract_id":close_cond["LS_contract_id"],"LS_cltr_amnt_out":close_cond["SYS_lease_amnt_LPN"]})
    if not open_ls_contracts.loc[open_ls_contracts["LS_contract_id"].isin(close_cond["LS_contract_id"])].empty:
        open_ls_contracts = open_ls_contracts.drop(open_ls_contracts.loc[open_ls_contracts["LS_contract_id"].isin(close_cond["LS_contract_id"])].index,axis=0)
        LS_Closing = pd.concat([LS_Closing,closing_record],axis=0)
    else:
        pass
    return open_ls_contracts,LS_Closing


def apply_rewards(timestamp,open_lp_contracts, TR_State, TR_Rewards_Distribution, nolus_price):
    state = TR_State.loc[TR_State["TR_timestamp"] == timestamp]
    reward = TR_Rewards_Distribution.loc[TR_Rewards_Distribution["TR_Rewards_timestamp"] == timestamp]
    if (state["TR_amnt_nls"] < reward["TR_Rewards_amnt_nls"].sum()).reset_index(drop=True)[0]:
        TR_Rewards_Distribution.loc[TR_Rewards_Distribution["TR_Rewards_timestamp"] == timestamp, "TR_Rewards_amnt_nls"] = 0
        open_lp_contracts["LP_Lender_rewards_nls_current"] = 0
    else:
        open_lp_contracts["LP_Lender_rewards_nls_current"] = open_lp_contracts["LP_Pool_id"].map(dict(open_lp_contracts[["LP_Pool_id","LP_amnt_stable"]].groupby("LP_Pool_id").sum().reset_index().values))
        open_lp_contracts["LP_Lender_rewards_nls_current"] = open_lp_contracts["LP_amnt_stable"]/open_lp_contracts["LP_Lender_rewards_nls_current"]
        open_lp_contracts["pool_rewards"] = open_lp_contracts["LP_Pool_id"].map(dict(reward[["TR_Rewards_Pool_id","TR_Rewards_amnt_nls"]].values))
        open_lp_contracts["LP_Lender_rewards_nls_current"] = open_lp_contracts["LP_Lender_rewards_nls_current"]*open_lp_contracts["pool_rewards"]
        open_lp_contracts["LP_Lender_rewards_nls_total"] = open_lp_contracts["LP_Lender_rewards_nls_total"] + open_lp_contracts["LP_Lender_rewards_nls_current"]
        open_lp_contracts = open_lp_contracts.drop("pool_rewards",axis=1)
        TR_State.loc[TR_State["TR_timestamp"] == timestamp, "TR_amnt_nls"] = TR_State.loc[TR_State[
                                                                                              "TR_timestamp"] == timestamp, "TR_amnt_nls"] - \
                                                                             open_lp_contracts["LP_Lender_rewards_nls_current"].sum()
        TR_State.loc[TR_State["TR_timestamp"] == timestamp, "TR_amnt_stable"] = TR_State.loc[TR_State[
                                                                                                 "TR_timestamp"] == timestamp, "TR_amnt_nls"] * \
                                                                                nolus_price.loc[nolus_price[
                                                                                                    "MP_timestamp"] == timestamp, "MP_price_in_stable"]
    open_lp_contracts["LP_Lender_rewards_stable"] = open_lp_contracts["LP_Lender_rewards_nls_total"]*nolus_price.loc[nolus_price["MP_timestamp"]==timestamp,"MP_price_in_stable"].values[0]

    return open_lp_contracts, TR_State, TR_Rewards_Distribution

def print_report(timestamp,pool_util,withdraw_sum,liquidation_sum,repayment_sum,repayment_principal,rewards):
    print("REPORT!")
    print("Timestamp:\n", timestamp)
    print("repayment_principal:\n", repayment_principal)
    print("withdraw_sum:\n", withdraw_sum)
    print("liquidation_sum:\n", liquidation_sum)
    print("repayment_sum:\n", repayment_sum)
    print("Pool_utilisation:\n", pool_util)
    print("Rewards spread:\n", rewards)

    return

def MC_dayli_calculcations(MP_Asset,LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, SYS_LP_Withdraw,
                          LP_Pool_State,LS_Closing, PL_State,TR_Profit,TR_State,TR_Rewards_Distribution,LS_State,nolus_price, pool_id, args):
    repayment_sum = 0
    liquidation_sum = 0
    withdraw_sum = 0
    repayment_principal=0
    symbol_id = {v: k for k, v in dict(pool_id.values).items()}
    i = 0
    open_ls_contracts = LS_State.copy()
    open_lp_contracts = SYS_LP_Withdraw.copy()
    PL_Interest = pd.DataFrame(
        {"PL_timestamp": [], "LP_Pool_id": [], "PL_borrowed_stable": [], "PL_deposited_stable": [], "Util": [],
         "LS_interest": [], "LP_interest": []})
    timestamps = get_timestamps(MP_Asset)
    pool_util = pd.DataFrame({"LP_Pool_id": pool_id["LP_Pool_id"], "Util": np.zeros(len(pool_id))})
    pool_interest = pd.DataFrame(
        {"LP_Pool_id": pool_id["LP_Pool_id"], "interest": np.ones(len(pool_id)) * args["base_interest"]/100})  # change from conf
    open_ls_contracts_prev = open_ls_contracts.copy()
    open_lp_contracts_prev = open_lp_contracts.copy()
    rewards_distributed_amnt = 0
    pool_interest_prev = pool_interest.copy()
    pool_util_prev = pool_util.copy()
    rewards_distributed_amnt_prev = 0
    withdraw_sum_prev = 0
    liquidation_sum_prev =0
    repayment_sum_prev = 0
    repayment_principal_prev = 0
    lpps_prev = LP_Pool_State.copy()
    while i <= len(timestamps)-1:
        timestamp = timestamps.values[i][0]
        if (i-1)<0:
            prev_timestamp = timestamp
        else:
            prev_timestamp = timestamps.values[i-1][0]
        i = i + 1
        LS_Opening,LS_Repayment,LS_Liquidation = ls_open_contracts(LS_Opening,LS_Repayment,LS_Liquidation, timestamp, pool_interest, args)
        open_ls_contracts, LS_Repayment, LS_Liquidation = ls_contract_manager(timestamp,open_ls_contracts,LS_Opening,LS_Repayment,LS_Liquidation,MP_Asset,pool_interest,args)
        open_lp_contracts, SYS_LP_Withdraw = lp_contract_manager(timestamp, open_lp_contracts, LP_Deposit, SYS_LP_Withdraw, pool_interest,args)
        open_ls_contracts,repayment_sum,repayment_principal,liquidation_sum,LS_Liquidation,LS_Closing = ls_event_manager(timestamp, open_ls_contracts, LS_Repayment, LS_Liquidation, LS_Closing,MP_Asset,symbol_id,pool_id,args)
        open_ls_contracts,LS_Closing = close_contracts(timestamp,open_ls_contracts,LS_Closing,LS_Repayment)
        withdraw_sum,open_lp_contracts,LP_Withdraw,SYS_LP_Withdraw = lp_event_manager(timestamp, open_lp_contracts, LP_Withdraw, SYS_LP_Withdraw, pool_id)
        withdraw_sum, liquidation_sum, repayment_sum, repayment_principal = additions_check(withdraw_sum, liquidation_sum, repayment_sum,repayment_principal,pool_id)
        LP_Pool_State_curr, LP_Pool_State = LP_Pool_State_update(timestamp, lpps_prev, LP_Pool_State, LS_Opening,
                                                                 LP_Deposit, MP_Asset, pool_id, repayment_sum,
                                                                 liquidation_sum, withdraw_sum, repayment_principal,
                                                                 args)

        PL_State= PL_State_update(prev_timestamp,timestamp,PL_State,LS_Opening,LS_Repayment,LS_Liquidation,LS_Closing,LP_Deposit,LP_Withdraw,args)
        TR_Profit = TR_Profit_update(timestamp, i,rewards_distributed_amnt,nolus_price, TR_Profit, LP_Pool_State, PL_State, LS_Opening, args)
        TR_State = TR_State_update(timestamp,nolus_price,prev_timestamp,TR_Profit,TR_State,args)
        TR_Rewards_Distribution = TR_Rewards_Distribution_update(timestamp,nolus_price,TR_Rewards_Distribution,LP_Pool_State,args)
        open_lp_contracts,TR_State,TR_Rewards_Distribution = apply_rewards(timestamp,open_lp_contracts,TR_State,TR_Rewards_Distribution,nolus_price)
        print_report(timestamp, pool_util, withdraw_sum, liquidation_sum, repayment_sum, repayment_principal,rewards=TR_Rewards_Distribution.loc[TR_Rewards_Distribution["TR_Rewards_timestamp"]==timestamp,"TR_Rewards_amnt_nls"].sum())
        add = LP_Pool_State.loc[LP_Pool_State["LP_Pool_timestamp"] == timestamp, ["LP_Pool_timestamp", "LP_Pool_id",
                                                                                  "LP_Pool_total_borrowed_stable",
                                                                                  "SYS_LP_Pool_TV_IntDep_stable"]]
        add = add.rename(columns={"LP_Pool_timestamp": "PL_timestamp", "LP_total_borrowed_stable": "PL_borrowed_stable",
                                  "SYS_LP_Pool_TV_IntDep_stable": "PL_deposited_stable"})
        add["Util"] = add["LP_Pool_id"].map(dict(pool_util.values))
        add["LS_interest"] = add["LP_Pool_id"].map(dict(pool_interest.values))
        #rewards_distributed_amnt = pool_rewards["pool_rewards"].sum()
        pool_interest, pool_util = calculate_interest(timestamp, LP_Pool_State, pool_util, args)
        nolus_price = nolus_price_adjustment(timestamp, pool_util, pool_util_prev, nolus_price, TR_Profit, args)
        pool_util, LS_Opening, LS_Repayment, LS_Liquidation, LP_Pool_State, check = check_util(timestamp, pool_util,
                                                                                               LS_Opening, LS_Repayment,
                                                                                               LS_Liquidation,
                                                                                               LP_Pool_State, args)



        if check:
            withdraw_sum = withdraw_sum_prev
            liquidation_sum = liquidation_sum_prev
            repayment_sum = repayment_sum_prev
            repayment_principal = repayment_principal_prev
            pool_interest = pool_interest_prev.copy()
            pool_util = pool_util_prev.copy()
            open_ls_contracts = open_ls_contracts_prev.copy()
            open_lp_contracts = open_lp_contracts_prev.copy()
            #rewards_distributed_amnt = rewards_distributed_amnt_prev.copy()
            i = i - 1
        else:
            withdraw_sum_prev = withdraw_sum.copy()
            liquidation_sum_prev = liquidation_sum.copy()
            repayment_sum_prev = repayment_sum.copy()
            repayment_principal_prev = repayment_principal.copy()
            pool_interest_prev = pool_interest.copy()
            pool_util_prev = pool_util.copy()
            lpps_prev = LP_Pool_State_curr.copy()
            open_ls_contracts_prev = open_ls_contracts.copy()
            open_lp_contracts_prev = open_lp_contracts.copy()
            #rewards_distributed_amnt_prev = rewards_distributed_amnt.copy()
            LS_State = pd.concat([LS_State,open_ls_contracts],axis=0)
            SYS_LP_Withdraw = pd.concat([open_lp_contracts, SYS_LP_Withdraw], axis=0)
            PL_Interest = PL_Interest.append(add)
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

    return LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, LP_Pool_State, SYS_LP_Withdraw, PL_Interest, LS_Closing,PL_State,TR_State,TR_Profit,TR_Rewards_Distribution,LS_State,nolus_price

