import pandas as pd
import numpy as np
#todo: create other LS_Closing function for the correct purpose
from modules.LS_Closing import LS_Closing_market_cond_update
import datetime

def LS_State_ini(LS_Opening,args):
    #lss_timestamp(LS_Opening, args)
    #LS_amnt_stable - dayly changing, = LS_amnt_asset*price
    LS_State = pd.DataFrame({"LS_timestamp":[],"LS_contract_id":[],"LP_Pool_id":[],"LS_asset_symbol":[],"LS_amnt_stable":[],
                             "LS_amnt_asset":[],"SYS_asset_price_stable":[],"LS_principal_stable":[], "LS_current_margin_stable":[],\
                             "LS_current_interest_stable":[], "LS_prev_margin_stable":[], "LS_prev_interest_stable":[],\
                             "LS_loan_amnt":[],"SYS_liability_LPN":[],"SYS_lease_amnt_LPN":[]})#lss_timestamp(LS_Opening, args)
    #{"LS_amnt_stable":[],"LS_prev_margin_stable":[],
    # "LS_prev_interest_stable":[],"LS_current_margin_stable":[],"LS_current_interest_stable":[],"LS_principal_stable":[],"SYS_LS_current_collateral_stable":[]
    return LS_State


def ls_event_handler(timestamp, MP_Asset, LS_State, LS_Repayment, LS_Liquidation,args):
    #MAin goal
    #check for event ocurrancies on daily basis - payments, and aplies changes in LS_State
    #assuming that LS_Repayment and LS_Liquidation contain only records for currently open contracts in the current timestamp
    # - no closed contract records in LS_Repayment and LS_Liquidation
    #rep = LS_Repayment#[["LS_timestamp", "LS_contract_id", "LS_amnt_stable", "LS_principal_stable", "LS_current_margin_stable", "LS_current_interest_stable","LS_prev_margin_stable","LS_prev_interest_stable"]].copy()
    #rep = rep.rename(columns={"LS_amnt_stable":"LS_amnt_stable_rep"})
    #liq = LS_Liquidation#[["LS_timestamp", "LS_contract_id", "LS_amnt_stable","SYS_LS_asset_symbol","SYS_LS_cltr_ini","SYS_LS_cltr_amnt_taken"]].copy()
    #liq = liq.rename(columns={"LS_amnt_stable": "LS_amnt_stable_liq"})
    rep = LS_Repayment.loc[LS_Repayment["LS_timestamp"]==timestamp]
    liq = LS_Liquidation.loc[LS_Liquidation["LS_timestamp"]==timestamp]
    if not rep.empty or not liq.empty:
        #todo: remove aggregations
        # split by timestamp and drop rows from LS_State , apply actions and concat
        #   create
        temp = LS_State.loc[LS_State["LS_timestamp"]==timestamp]
        temp = temp.loc[temp["LS_contract_id"].isin(rep["LS_contract_id"])]
        LS_State = LS_State.drop(temp.index,axis=0)
        temp["SYS_rep_values"] = temp["LS_contract_id"].map(dict(rep[["LS_contract_id","LS_amnt_stable"]].values))
        temp["SYS_liq_values"] = temp["LS_contract_id"].map(dict(liq[["LS_contract_id","LS_amnt_stable"]].values)).fillna(0)
        temp["SYS_principal_values"] = temp["LS_contract_id"].map(dict(rep[["LS_contract_id","LS_principal_stable"]].values))
        temp["LS_principal_stable"] = temp["LS_principal_stable"] - temp["SYS_principal_values"]
        temp["LS_current_margin_stable"] = temp["LS_contract_id"].map(dict(rep[["LS_contract_id","LS_current_interest_stable"]].values))
        temp["LS_current_interest_stable"]=temp["LS_current_margin_stable"]
        temp["LS_prev_margin_stable"] = temp["LS_current_margin_stable"]
        temp["LS_prev_interest_stable"]=temp["LS_current_margin_stable"]
        temp["LS_amnt_stable"] = temp["LS_amnt_stable"] - temp["SYS_liq_values"].fillna(0) - temp["SYS_rep_values"]
        #temp = temp.drop(columns={"SYS_rep_values","SYS_liq_values","SYS_principal_values"},axis=1)
        LS_State = pd.concat([LS_State,temp],axis=0,ignore_index=True)
    assets = MP_Asset.loc[MP_Asset["MP_timestamp"]==timestamp]
    LS_State.loc[LS_State["LS_timestamp"] == timestamp, "SYS_LS_price_in_stable"] = LS_State.loc[LS_State["LS_timestamp"] == timestamp, "LS_asset_symbol"].map(dict(assets[["MP_asset_symbol","MP_price_in_stable"]].values))
    symbol_digit = pd.DataFrame(args["symbol_digit"])
    symbol_digit = symbol_digit.rename(columns={"symbol":"LS_asset_symbol"})
    LS_State = pd.merge(LS_State,symbol_digit, on="LS_asset_symbol",how='left')
    LS_State.loc[LS_State["LS_timestamp"] == timestamp, "LS_cltr_amnt"] = LS_State.loc[LS_State["LS_timestamp"] == timestamp, "LS_cltr_amnt"] -  LS_State.loc[LS_State["LS_timestamp"]==timestamp,"LS_contract_id"].map(dict(liq[["LS_contract_id","SYS_LS_cltr_amnt_taken"]].values)).fillna(0)
    LS_State.loc[LS_State["LS_timestamp"] == timestamp, "SYS_LS_staked_cltr_in_stable"] = (LS_State.loc[LS_State["LS_timestamp"]==timestamp,"LS_cltr_amnt"]/(10**LS_State.loc[LS_State["LS_timestamp"] == timestamp, "digit"]))*LS_State.loc[LS_State["LS_timestamp"]==timestamp,"SYS_LS_price_in_stable"]
    LS_State = LS_State.drop("digit",axis=1)
    return LS_State


def liq_record_creation(timestamp,contracts_to_close,LS_Liquidation,pool_id):
    #create liquidation record for the current timestamp to be capitalized on when calculating repayment and liquidation sum
    #create closing record on the current timestamp
    #LS_timestamp,LS_contract_id,LS_symbol,LS_amnt_stable,LS_principal_stable,LS_current_margin_stable,
    #LS_current_interest_stable,LS_prev_interest_stable,LS_prev_margin_stable,LS_transaction_type,SYS_LS_asset_symbol,SYS_LS_cltr_ini,
    # SYS_LS_cltr_price,SYS_LS_cltr_amnt_taken,LS_liquidation_height,LS_liquidation_idx
    liq_records = pd.DataFrame({"LS_timestamp":np.repeat(timestamp,len(contracts_to_close["LS_contract_id"])),"LS_contract_id":contracts_to_close["LS_contract_id"],"LS_symbol":contracts_to_close["LP_Pool_id"].map(dict(pool_id[["LP_Pool_id","LP_symbol"]].values)),
                                "LS_principal_stable":np.repeat(0,len(contracts_to_close["LS_contract_id"])),"LS_current_margin_stable":np.repeat(0,len(contracts_to_close["LS_contract_id"]))
                                   ,
                                "LS_current_interest_stable":np.repeat(0,len(contracts_to_close["LS_contract_id"])),"LS_prev_interest_stable":np.repeat(0,len(contracts_to_close["LS_contract_id"])),
                                "LS_prev_margin_stable":np.repeat(0,len(contracts_to_close["LS_contract_id"])),"LS_transaction_type":np.repeat(3,len(contracts_to_close["LS_contract_id"])),
                                "LS_asset_symbol":contracts_to_close["LS_asset_symbol"],"LS_asset_price":contracts_to_close["SYS_asset_price_stable"]})
    liq_records["LS_amnt_stable"] = contracts_to_close["liq_amnt"]
    num = LS_Liquidation["LS_liquidation_idx"].max() + 1
    height = [num + s for s in
                       range(0,len(liq_records))]
    liq_records["LS_liquidation_height"] = height
    liq_records["LS_liquidation_idx"] = height
    LS_Liquidation = pd.concat((LS_Liquidation,liq_records),axis=0,ignore_index=True)
    #LS_Closing = LS_Closing_market_cond_update(timestamp, LS_Closing, LS_Liquidation)

    return LS_Liquidation#,LS_Closing


#todo: change to liquidation event handler - split contracts in 2 partial liquidation and full liquidation , apply changes
def closing_event_handler(timestamp,open_contracts,LS_State,LS_Repayment,LS_Liquidation, LS_Closing,LS_Opening,args):
    #check if LS_amnt_stable < SYS_LS_staked_cltr_in_stable
    timest_contracts = LS_State.loc[LS_State["LS_timestamp"] == timestamp]
    close_cond = (timest_contracts["SYS_LS_staked_cltr_in_stable"]*args["max_cltr_percent"]/100)<(timest_contracts["LS_principal_stable"] + timest_contracts["LS_current_margin_stable"]+timest_contracts["LS_current_interest_stable"]+timest_contracts["LS_prev_margin_stable"]+timest_contracts["LS_prev_interest_stable"])
    partial_liq_cond = (timest_contracts["SYS_LS_staked_cltr_in_stable"]*args["healthy_cltr_percent"]/100)<(timest_contracts["LS_principal_stable"] + timest_contracts["LS_current_margin_stable"]+timest_contracts["LS_current_interest_stable"]+timest_contracts["LS_prev_margin_stable"]+timest_contracts["LS_prev_interest_stable"])
    contracts_to_close = timest_contracts.loc[close_cond]
    contracts_to_partial_liquidate = timest_contracts.loc[partial_liq_cond]
    contracts_to_partial_liquidate = contracts_to_partial_liquidate.loc[~contracts_to_partial_liquidate["LS_contract_id"].isin(contracts_to_close["LS_contract_id"])]
    if contracts_to_close.empty and contracts_to_partial_liquidate.empty:
        return open_contracts,LS_State,LS_Repayment,LS_Liquidation, LS_Closing

    else:
        if not contracts_to_close.empty:
            rep = LS_Repayment.loc[LS_Repayment["LS_timestamp"] > timestamp]
            contracts_to_close["LS_symbol"] = contracts_to_close["LS_contract_id"].map(dict(rep.drop_duplicates(subset="LS_contract_id",keep="last")[["LS_contract_id","LS_symbol"]].values))
            LS_Repayment = LS_Repayment.drop(rep.loc[rep["LS_contract_id"].isin(contracts_to_close["LS_contract_id"])].index,axis=0)
            liq = LS_Liquidation.loc[LS_Liquidation["LS_timestamp"] > timestamp]
            LS_Liquidation = LS_Liquidation.drop(liq.loc[liq["LS_contract_id"].isin(contracts_to_close["LS_contract_id"])].index, axis=0)
            LS_Liquidation,LS_Closing = liq_closing_record_creation(timestamp,contracts_to_close,LS_Liquidation,LS_Closing,LS_Opening,args)
    return open_contracts,LS_State, LS_Repayment, LS_Liquidation, LS_Closing

def partial_liq(timestamp,LS_State,LS_Repayment,LS_Liquidation,LS_Opening,args):
    timest_contracts = LS_State.loc[LS_State["LS_timestamp"] == timestamp]
    close_cond = (timest_contracts["SYS_LS_staked_cltr_in_stable"]*args["max_cltr_percent"]/100)<(timest_contracts["LS_principal_stable"] + timest_contracts["LS_current_margin_stable"]+timest_contracts["LS_current_interest_stable"]+timest_contracts["LS_prev_margin_stable"]+timest_contracts["LS_prev_interest_stable"])
    partial_liq_cond = (timest_contracts["SYS_LS_staked_cltr_in_stable"]*args["healthy_cltr_percent"]/100)<(timest_contracts["LS_principal_stable"] + timest_contracts["LS_current_margin_stable"]+timest_contracts["LS_current_interest_stable"]+timest_contracts["LS_prev_margin_stable"]+timest_contracts["LS_prev_interest_stable"])
    contracts_to_close = timest_contracts.loc[close_cond]
    contracts_to_partial_liquidate = timest_contracts.loc[partial_liq_cond]
    contracts_to_partial_liquidate = contracts_to_partial_liquidate.loc[~contracts_to_partial_liquidate["LS_contract_id"].isin(contracts_to_close["LS_contract_id"])]
    if not contracts_to_partial_liquidate.empty:

        # todo: calculate liquidation transacrions
        # (liabilityLPN(time) - LeaseHealthyLiability% / 100 * leaseAmountLPN(time)) / (1 - LeaseHealthyLiability% / 100))
        # current_principal_post_liquidation = current_princ_preliquidation - Borrow(time)/(Borrow(time) + Cltr(time)) * LiquidationAmountLPN
        # current_colateral_to_liquidate = (1 - Borrow(time)/(Borrow(time) + Cltr(time))) * LiquidationAmountLPN
        # IN CONTRACTS_TO_LIQUIDATE are left only the values for the liquidation transaction !!!
        rep = LS_Repayment.loc[LS_Repayment["LS_timestamp"] > timestamp]
        contracts_to_partial_liquidate["LS_symbol"] = contracts_to_partial_liquidate["LS_contract_id"].map(
            dict(rep.drop_duplicates(subset="LS_contract_id", keep="last")[["LS_contract_id", "LS_symbol"]].values))
        contracts_to_partial_liquidate["total_value"] = contracts_to_partial_liquidate["LS_principal_stable"] + \
                                                        contracts_to_partial_liquidate["SYS_LS_staked_cltr_in_stable"]
        contracts_to_partial_liquidate["liabilityLPN"] = contracts_to_partial_liquidate["LS_principal_stable"] + \
                                                         contracts_to_partial_liquidate["LS_current_margin_stable"] + \
                                                         contracts_to_partial_liquidate["LS_current_interest_stable"] + \
                                                         contracts_to_partial_liquidate["LS_prev_margin_stable"] + \
                                                         contracts_to_partial_liquidate["LS_prev_interest_stable"]
        contracts_to_partial_liquidate["borowed_to_cltr"] = contracts_to_partial_liquidate["LS_principal_stable"] / (
                contracts_to_partial_liquidate["LS_principal_stable"] + contracts_to_partial_liquidate[
            "SYS_LS_staked_cltr_in_stable"])
        # todo:post_liquidation !
        contracts_to_partial_liquidate["post_liq_val"] = contracts_to_partial_liquidate["LS_principal_stable"] - (
              contracts_to_partial_liquidate["LS_principal_stable"] / contracts_to_partial_liquidate[
         "total_value"]) * ((contracts_to_partial_liquidate["liabilityLPN"] - (
            args["healthy_cltr_percent"] / 100) * contracts_to_partial_liquidate["LS_principal_stable"]) / (
                                  1 - args["healthy_cltr_percent"] / 100))
        contracts_to_partial_liquidate["post_to_pre"] = contracts_to_partial_liquidate["post_liq_val"] / \
                                                        contracts_to_partial_liquidate["LS_principal_stable"]
        contracts_to_partial_liquidate["SYS_LS_staked_cltr_in_stable_left"] = (1 - contracts_to_partial_liquidate[
            "borowed_to_cltr"]) * contracts_to_partial_liquidate["post_to_pre"] * contracts_to_partial_liquidate[
                                                                                  "SYS_LS_staked_cltr_in_stable"]
        contracts_to_partial_liquidate["SYS_LS_staked_cltr_in_stable"] = contracts_to_partial_liquidate[
                                                                             "SYS_LS_staked_cltr_in_stable"] - \
                                                                         contracts_to_partial_liquidate[
                                                                             "SYS_LS_staked_cltr_in_stable_left"]
        contracts_to_partial_liquidate["LS_principal_stable_left"] = contracts_to_partial_liquidate["borowed_to_cltr"] * \
                                                                     contracts_to_partial_liquidate["post_to_pre"] * \
                                                                     contracts_to_partial_liquidate[
                                                                         "LS_principal_stable"]
        contracts_to_partial_liquidate["LS_principal_stable"] = contracts_to_partial_liquidate["LS_principal_stable"] - \
                                                                contracts_to_partial_liquidate[
                                                                    "LS_principal_stable_left"]
        contracts_to_partial_liquidate["coef"] = contracts_to_partial_liquidate["borowed_to_cltr"] * \
                                                 contracts_to_partial_liquidate["post_to_pre"]
        LS_Repayment["SYS_LS_coef"] = LS_Repayment["LS_contract_id"].map(
            dict(contracts_to_partial_liquidate[["LS_contract_id", "coef"]].values))
        LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(
            contracts_to_partial_liquidate["LS_contract_id"]), "LS_principal_stable"] = LS_Repayment.loc[LS_Repayment[
                                                                                                             "LS_contract_id"].isin(
            contracts_to_partial_liquidate["LS_contract_id"]), "LS_principal_stable"] * LS_Repayment.loc[LS_Repayment[
                                                                                                             "LS_contract_id"].isin(
            contracts_to_partial_liquidate["LS_contract_id"]), "SYS_LS_coef"]
        # liquidation transaction :
        LS_Liquidation = liq_transaction_creation(timestamp, contracts_to_partial_liquidate, LS_Liquidation, LS_Opening,
                                                  args)
        # fix contracts for ls_event_handler
        contracts_to_partial_liquidate["LS_principal_stable"] = contracts_to_partial_liquidate[
            "LS_principal_stable_left"]
        contracts_to_partial_liquidate["SYS_LS_staked_cltr_in_stable"] = contracts_to_partial_liquidate[
            "SYS_LS_staked_cltr_in_stable_left"]
        LS_State = LS_State.drop(LS_State.loc[LS_State["LS_contract_id"].isin(contracts_to_partial_liquidate["LS_contract_id"])].index,axis=0)
        LS_State = pd.concat((LS_State,contracts_to_partial_liquidate),axis=0)
    return LS_State,LS_Repayment,LS_Liquidation


def lss_timestamp(LS_Opening, args):
    day = 86400
    duration = (LS_Opening["SYS_LS_expected_payment"]+LS_Opening["SYS_LS_expected_penalty"])
    duration = duration.values * 30
    timestamp = pd.DataFrame({"LS_timestamp": LS_Opening["LS_timestamp"], "LS_contract_id": LS_Opening["LS_contract_id"]})
    timestamp["LS_timestamp"] = LS_Opening["LS_timestamp"].astype("datetime64").apply(lambda x: datetime.datetime.timestamp(x)).astype(int)
    timestamp = pd.DataFrame(np.repeat(timestamp.values, duration, axis=0))
    timestamp = pd.DataFrame(
        {"LS_timestamp": timestamp[0], "LS_contract_id": timestamp[1],})
    timestamp["LS_timestamp"] = timestamp["LS_timestamp"].values.astype("uint64")
    # duration = pd.DataFrame({"duration":duration})
    d = []
    l1 = []
    for i in duration:
        l1 = [s for s in range(1, i + 1)]
        d.extend(l1)
    timestamp["Duration"] = d
    timestamp["Duration"] = timestamp["Duration"] * day
    timestamp["LS_timestamp"] = timestamp["LS_timestamp"] + timestamp["Duration"]
    timestamp["LS_timestamp"] = timestamp["LS_timestamp"].apply(datetime.datetime.fromtimestamp)
    timestamp = timestamp.drop("Duration", axis=1)

    return timestamp
