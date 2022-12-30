import pandas as pd
import numpy as np
#todo: create other LS_Closing function for the correct purpose
from modules.LS_Closing import LS_Closing_market_cond_update
import datetime

def LS_State_ini(LS_Opening,args):
    #lss_timestamp(LS_Opening, args)
    LS_State = pd.DataFrame({"LS_timestamp":[],"LS_contract_id":[],"LS_amnt_stable":[],"LS_principal_stable":[], "LS_current_margin_stable":[],\
                             "LS_current_interest_stable":[], "LS_prev_margin_stable":[], "LS_prev_interest_stable":[],"LS_asset_symbol":[],\
                             "LS_cltr_amnt":[],"SYS_LS_price_in_stable":[],"SYS_LS_staked_cltr_in_stable":[]})#lss_timestamp(LS_Opening, args)
    #{"LS_amnt_stable":[],"LS_prev_margin_stable":[],
    # "LS_prev_interest_stable":[],"LS_current_margin_stable":[],"LS_current_interest_stable":[],"LS_principal_stable":[],"SYS_LS_current_collateral_stable":[]
    return LS_State

def check_open(open_contracts, LS_State):
    #returns all contracts that need initialization with open contract
    not_open_contracts = open_contracts.loc[~open_contracts["LS_contract_id"].isin(LS_State["LS_contract_id"])]
    return not_open_contracts

def open_contract(timestamp, contracts_to_open, MP_Asset, LS_Opening, LS_State, LS_Repayment, LS_Liquidation, args):
    #Main goal:
    #Create initialisation record for each contract in "contracts_to_open" for the specific timestamp
    #These init recors will contain all the init data for the record/contract/ to be edited in later stages
    #numpy.reapeat(3, 10)
    contract_margins = LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(contracts_to_open["LS_contract_id"]),["LS_contract_id","LS_current_margin_stable","LS_current_interest_stable","LS_prev_margin_stable","LS_prev_interest_stable"]].drop_duplicates(subset="LS_contract_id",keep='first')
    df = pd.DataFrame({"LS_timestamp":np.repeat(timestamp,len(contracts_to_open["LS_contract_id"])),"LS_contract_id":contracts_to_open["LS_contract_id"].values})
    df["LS_principal_stable"] = df["LS_contract_id"].map(dict(LS_Opening.loc[LS_Opening["LS_contract_id"].isin(df["LS_contract_id"]),["LS_contract_id","LS_loan_amnt_stable"]].values))
    df["LS_interest"] = df["LS_contract_id"].map(dict(LS_Opening.loc[LS_Opening["LS_contract_id"].isin(df["LS_contract_id"]),["LS_contract_id","LS_interest"]].values))
    df["LS_asset_symbol"] = df["LS_contract_id"].map(dict(LS_Opening.loc[
        LS_Opening["LS_contract_id"].isin(df["LS_contract_id"]), ["LS_contract_id","LS_asset_symbol"]].values))
    df["LS_amnt_stable"] = df["LS_principal_stable"] + df["LS_principal_stable"]*df["LS_interest"]
    df["LS_cltr_amnt"] = df["LS_contract_id"].map(dict(LS_Opening.loc[
                                                             LS_Opening["LS_contract_id"].isin(df["LS_contract_id"]), [
                                                                 "LS_contract_id", "LS_cltr_amnt_asset"]].values))
    df = pd.merge(df,contract_margins,on="LS_contract_id",how='left')

    df["SYS_LS_price_in_stable"] = df["LS_asset_symbol"].map(dict(MP_Asset.loc[MP_Asset["MP_timestamp"]==timestamp,["MP_asset_symbol","MP_price_in_stable"]].values))
    df = df.drop("LS_interest",axis=1)

    LS_State = pd.concat([LS_State,df],ignore_index=True,axis=0)
    #LS_principal- loan amnt, LS_amnt - loan + interest, SYS_LS_price_in_stable - MP_asset price for timestamp

    return LS_State

def replicate_open_contracts(timestamp, prev_timestamp, LS_State, open_contracts):
    #Replicates records of the open contracts from the last timestamp and changes the timestamp to current timestamp
    df = LS_State.loc[LS_State["LS_timestamp"]==prev_timestamp].copy()
    df = df.loc[df["LS_contract_id"].isin(open_contracts["LS_contract_id"])]
    df["LS_timestamp"] = timestamp
    LS_State = pd.concat([LS_State,df],axis=0)
    return LS_State

def ls_event_handler(timestamp, MP_Asset, LS_State, LS_Repayment, LS_Liquidation,args):
    #MAin goal
    #check for event ocurrancies on daily basis - payments, and aplies changes in LS_State
    #assuming that LS_Repayment and LS_Liquidation contain only records for currently open contracts in the current timestamp
    # - no closed contract records in LS_Repayment and LS_Liquidation
    rep = LS_Repayment#[["LS_timestamp", "LS_contract_id", "LS_amnt_stable", "LS_principal_stable", "LS_current_margin_stable", "LS_current_interest_stable","LS_prev_margin_stable","LS_prev_interest_stable"]].copy()
    #rep = rep.rename(columns={"LS_amnt_stable":"LS_amnt_stable_rep"})
    liq = LS_Liquidation#[["LS_timestamp", "LS_contract_id", "LS_amnt_stable","SYS_LS_asset_symbol","SYS_LS_cltr_ini","SYS_LS_cltr_amnt_taken"]].copy()
    #liq = liq.rename(columns={"LS_amnt_stable": "LS_amnt_stable_liq"})
    rep = rep.loc[rep["LS_timestamp"]==timestamp]
    liq = liq.loc[liq["LS_timestamp"]==timestamp]
    if not rep.empty or not liq.empty:
        #todo: remove aggregations
        # split by timestamp and drop rows from LS_State , apply actions and concat
        temp = LS_State.loc[LS_State["LS_timestamp"]==timestamp].copy()
        LS_State = LS_State.drop(LS_State.loc[LS_State["LS_timestamp"]==timestamp].index,axis=0)
        temp["SYS_rep_values"] = temp["LS_contract_id"].map(rep[["LS_contract_id","LS_amnt_stable"]].values).fillna(0)
        temp["SYS_liq_values"] = temp["LS_contract_id"].map(liq[["LS_contract_id","LS_amnt_stable"]].values).fillna(0)
        temp["SYS_principal_values"] = temp["LS_contract_id"].map(rep[["LS_contract_id","LS_principal_stable"]].values).fillna(0)
        temp["LS_principal_stable"] = temp["LS_principal_stable"] - temp["SYS_principal_values"]

        #drop
        temp = temp.drop(columns={"SYS_rep_values","SYS_liq_values","LS_principal_stable"},axis=1)
        LS_State.loc[LS_State["LS_timestamp"]==timestamp,"SYS_rep_values"] = LS_State.loc[LS_State["LS_timestamp"]==timestamp,"LS_contract_id"].map(dict(rep[["LS_contract_id","LS_amnt_stable"]].values))
        LS_State.loc[LS_State["LS_timestamp"] == timestamp, "SYS_rep_values"] =LS_State.loc[LS_State["LS_timestamp"]==timestamp,"SYS_rep_values"].fillna(0)
        LS_State.loc[LS_State["LS_timestamp"]==timestamp,"SYS_liq_values"] = LS_State.loc[LS_State["LS_timestamp"]==timestamp,"LS_contract_id"].map(dict(liq[["LS_contract_id","LS_amnt_stable"]].values))
        LS_State.loc[LS_State["LS_timestamp"] == timestamp, "SYS_liq_values"] =LS_State.loc[LS_State["LS_timestamp"]==timestamp,"SYS_liq_values"].fillna(0)
        LS_State.loc[LS_State["LS_timestamp"]==timestamp,"LS_amnt_stable"] = LS_State.loc[LS_State["LS_timestamp"]==timestamp,"LS_amnt_stable"] \
                                                                         -  LS_State.loc[LS_State["LS_timestamp"]==timestamp,"SYS_liq_values"] -  LS_State.loc[LS_State["LS_timestamp"]==timestamp,"SYS_rep_values"]
        LS_State.loc[LS_State["LS_timestamp"] == timestamp, "LS_principal_stable"] = LS_State.loc[LS_State["LS_timestamp"] == timestamp, "LS_principal_stable"] - LS_State.loc[LS_State["LS_timestamp"]==timestamp,"LS_contract_id"].map(dict(rep[["LS_contract_id","LS_principal_stable"]].values)).fillna(0)
        LS_State.loc[LS_State["LS_timestamp"]==timestamp,"LS_current_margin_stable"] = LS_State.loc[LS_State["LS_timestamp"]==timestamp,"LS_contract_id"].map(dict(rep[["LS_contract_id","LS_current_margin_stable"]].values)).fillna(0)
        LS_State.loc[LS_State["LS_timestamp"]==timestamp,"LS_current_interest_stable"] = LS_State.loc[LS_State["LS_timestamp"]==timestamp,"LS_contract_id"].map(dict(rep[["LS_contract_id","LS_current_interest_stable"]].values)).fillna(0)
        LS_State.loc[LS_State["LS_timestamp"]==timestamp,"LS_prev_margin_stable"] = LS_State.loc[LS_State["LS_timestamp"]==timestamp,"LS_contract_id"].map(dict(rep[["LS_contract_id","LS_prev_margin_stable"]].values)).fillna(0)
        LS_State.loc[LS_State["LS_timestamp"]==timestamp,"LS_prev_interest_stable"] = LS_State.loc[LS_State["LS_timestamp"]==timestamp,"LS_contract_id"].map(dict(rep[["LS_contract_id","LS_prev_interest_stable"]].values)).fillna(0)
    assets = MP_Asset.loc[MP_Asset["MP_timestamp"]==timestamp]
    LS_State.loc[LS_State["LS_timestamp"] == timestamp, "SYS_LS_price_in_stable"] = LS_State.loc[LS_State["LS_timestamp"] == timestamp, "LS_asset_symbol"].map(dict(assets[["MP_asset_symbol","MP_price_in_stable"]].values))
    symbol_digit = pd.DataFrame(args["symbol_digit"])
    symbol_digit = symbol_digit.rename(columns={"symbol":"LS_asset_symbol"})
    LS_State = pd.merge(LS_State,symbol_digit, on="LS_asset_symbol",how='left')
    LS_State.loc[LS_State["LS_timestamp"] == timestamp, "LS_cltr_amnt"] = LS_State.loc[LS_State["LS_timestamp"] == timestamp, "LS_cltr_amnt"] -  LS_State.loc[LS_State["LS_timestamp"]==timestamp,"LS_contract_id"].map(dict(liq[["LS_contract_id","SYS_LS_cltr_amnt_taken"]].values)).fillna(0)
    LS_State.loc[LS_State["LS_timestamp"]==timestamp, "SYS_LS_staked_cltr_in_stable"] = (LS_State.loc[LS_State["LS_timestamp"]==timestamp,"LS_cltr_amnt"]/(10**LS_State.loc[LS_State["LS_timestamp"] == timestamp, "digit"]))*LS_State.loc[LS_State["LS_timestamp"]==timestamp,"SYS_LS_price_in_stable"]
    LS_State = LS_State.drop("digit",axis=1)
    return LS_State


def liq_record_creation(timestamp,contracts_to_close,LS_Liquidation,LS_Closing,LS_Opening,args):
    #create liquidation record for the current timestamp to be capitalized on when calculating repayment and liquidation sum
    #create closing record on the current timestamp
    #LS_timestamp,LS_contract_id,LS_symbol,LS_amnt_stable,LS_principal_stable,LS_current_margin_stable,
    #LS_current_interest_stable,LS_prev_interest_stable,LS_prev_margin_stable,LS_transaction_type,SYS_LS_asset_symbol,SYS_LS_cltr_ini,
    # SYS_LS_cltr_price,SYS_LS_cltr_amnt_taken,LS_liquidation_height,LS_liquidation_idx
    liq_records = pd.DataFrame({"LS_timestamp":np.repeat(timestamp,len(contracts_to_close["LS_contract_id"])),"LS_contract_id":contracts_to_close["LS_contract_id"],"LS_symbol":contracts_to_close["LS_symbol"],
                                "LS_principal_stable":contracts_to_close["LS_principal_stable"],"LS_current_margin_stable":contracts_to_close["LS_current_margin_stable"],
                                "LS_current_interest_stable":contracts_to_close["LS_current_interest_stable"],"LS_prev_interest_stable":contracts_to_close["LS_prev_interest_stable"],
                                "LS_prev_margin_stable":contracts_to_close["LS_prev_margin_stable"],"LS_transaction_type":np.repeat(2,len(contracts_to_close["LS_contract_id"])),
                                "SYS_LS_asset_symbol":contracts_to_close["LS_asset_symbol"],"SYS_LS_cltr_price":contracts_to_close["SYS_LS_price_in_stable"],
                                "SYS_LS_cltr_amnt_taken":contracts_to_close["LS_cltr_amnt"],})
    liq_records["LS_amnt_stable"] = liq_records["SYS_LS_cltr_amnt_taken"]*liq_records["SYS_LS_cltr_price"]
    symbol_digit = pd.DataFrame(args["symbol_digit"])
    symbol_digit = symbol_digit.rename(columns={"symbol":"SYS_LS_asset_symbol"})
    liq_records = pd.merge(liq_records,symbol_digit, on="SYS_LS_asset_symbol")
    liq_records["LS_amnt_stable"] = liq_records["LS_amnt_stable"]/10**liq_records["digit"]
    liq_records = liq_records.drop(columns="digit",axis=1)
    liq_records["SYS_LS_cltr_ini"] = liq_records["LS_contract_id"].map(dict(LS_Opening[["LS_contract_id","LS_cltr_amnt_asset"]].values))
    num = LS_Liquidation["LS_liquidation_idx"].max() + 1
    height = [num + s for s in
                       range(0,len(liq_records))]
    liq_records["LS_liquidation_height"] = height
    liq_records["LS_liquidation_idx"] = height
    LS_Liquidation = pd.concat((LS_Liquidation,liq_records),axis=0,ignore_index=True)
    LS_Closing = LS_Closing_market_cond_update(timestamp, LS_Closing, LS_Liquidation, LS_Opening)
    return LS_Liquidation,LS_Closing


def closing_event_handler(timestamp,open_contracts,LS_State,LS_Repayment,LS_Liquidation, LS_Closing,LS_Opening,args):
    #check if LS_amnt_stable < SYS_LS_staked_cltr_in_stable
    timest_contracts = LS_State.loc[LS_State["LS_timestamp"] == timestamp]
    cond = (timest_contracts["SYS_LS_staked_cltr_in_stable"]*args["healthy_cltr_percent"]/100)<(timest_contracts["LS_principal_stable"] + timest_contracts["LS_current_margin_stable"]+timest_contracts["LS_current_interest_stable"]+timest_contracts["LS_prev_margin_stable"]+timest_contracts["LS_prev_interest_stable"])
    contracts_to_close = timest_contracts.loc[cond]
    if contracts_to_close.empty:
        return open_contracts,LS_State,LS_Repayment,LS_Liquidation, LS_Closing
    else:
        rep = LS_Repayment.loc[LS_Repayment["LS_timestamp"] > timestamp]
        contracts_to_close["LS_symbol"] = contracts_to_close["LS_contract_id"].map(dict(rep.drop_duplicates(subset="LS_contract_id",keep="last")[["LS_contract_id","LS_symbol"]].values))
        LS_Repayment = LS_Repayment.drop(rep.loc[rep["LS_contract_id"].isin(contracts_to_close["LS_contract_id"])].index,axis=0)
        liq = LS_Liquidation.loc[LS_Liquidation["LS_timestamp"] > timestamp]
        LS_Liquidation = LS_Liquidation.drop(liq.loc[liq["LS_contract_id"].isin(contracts_to_close["LS_contract_id"])].index, axis=0)
        LS_Liquidation,LS_Closing = liq_record_creation(timestamp,contracts_to_close,LS_Liquidation,LS_Closing,LS_Opening,args)
        #open_contracts = open_contracts.drop([open_contracts.loc[open_contracts["LS_contract_id"].isin(contracts_to_close["LS_contract_id"])].index],axis=0)
    return open_contracts,LS_State, LS_Repayment, LS_Liquidation, LS_Closing

def LS_State_update(timestamp, prev_timestamp, open_contracts, MP_Asset, LS_State, LS_Opening, LS_Repayment, LS_Liquidation,LS_Closing, args):
    #MAIN GOAL:
    #catches events based on market condition and apply actions on all dependable tables - LS_Repayment, LS_Liquidation, LS_Closing
    # main mechanism is based on open_contracts which shows all open contracts for the current timestamp
    # update on open contracts is made in the end of the function after the update of the closing structure
    # and in the begining of every MC_daily cycle when we open contracts
    #1 - check for new contracts opened and create open record
    #2 replicate old records
    #3 calculate daily colateral
    #4 apply events based on LS_Repayment and LS_Liquidation
    #5 check for market condition
    #6 if market condition apply changes in LS_closing, LS_Repayment and LS_Liquidation
    new_contracts = LS_Opening.loc[LS_Opening["LS_timestamp"]==timestamp,["LS_contract_id"]]
    open_contracts = pd.concat([open_contracts,new_contracts],axis=0)
    contracts_to_open = check_open(open_contracts, LS_State)
    if not contracts_to_open.empty:
        LS_State = open_contract(timestamp, contracts_to_open, MP_Asset, LS_Opening, LS_State,LS_Repayment, LS_Liquidation, args)
    if not (prev_timestamp == timestamp) and not (prev_timestamp== None):
        LS_State = replicate_open_contracts(timestamp,prev_timestamp,LS_State,open_contracts)
    LS_State = ls_event_handler(timestamp, MP_Asset, LS_State, LS_Repayment, LS_Liquidation,args)
    open_contracts, LS_State, LS_Repayment, LS_Liquidation, LS_Closing = closing_event_handler(timestamp, open_contracts, LS_State, LS_Repayment, LS_Liquidation, LS_Closing, LS_Opening,args)
    open_contracts = open_contracts.loc[~open_contracts["LS_contract_id"].isin(LS_Closing.loc[LS_Closing["LS_timestamp"]==timestamp,"LS_contract_id"])]
    return open_contracts,LS_State,LS_Repayment,LS_Liquidation,LS_Closing


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
