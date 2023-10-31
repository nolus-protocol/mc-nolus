import pandas as pd

def LS_Closing_ini():
    LS_Closing = pd.DataFrame({"LS_timestamp":[],"LS_contract_id":[],"LS_cltr_amnt_out":[]})
    return LS_Closing

def LS_Closing_update(timestamp,LS_Closing,LS_Repayment):
    #select the last record in repayment for the given timestamp and create a closing record for it with all the data needed
    temp = LS_Repayment.drop_duplicates(subset=["LS_contract_id"],keep="last",ignore_index=True)
    temp = temp[["LS_timestamp","LS_contract_id"]]
    temp = temp.loc[temp["LS_timestamp"]==timestamp]

    LS_Closing = LS_Closing.drop(LS_Closing.loc[LS_Closing["LS_contract_id"].isin(temp["LS_contract_id"])].index)

    LS_Closing = pd.concat([LS_Closing,temp],axis=0,ignore_index=True)
    return LS_Closing

def LS_Closing_market_cond_update(timestamp, LS_Closing, LS_Liquidation):
    temp = LS_Liquidation.loc[LS_Liquidation["LS_timestamp"]==timestamp]
    temp = temp.loc[temp["LS_transaction_type"]==2,["LS_timestamp","LS_contract_id"]]
    drop_cond = (LS_Liquidation["LS_contract_id"].isin(temp["LS_contract_id"])) & (LS_Liquidation["LS_timestamp"] > timestamp)
    LS_Liquidation = LS_Liquidation.drop(LS_Liquidation[drop_cond].index,axis=0)
    LS_Liquidation = LS_Liquidation.reset_index(drop=True)
    temp["LS_cltr_amnt_out"] = 0
    LS_Closing = LS_Closing.drop(LS_Closing.loc[LS_Closing["LS_contract_id"].isin(temp["LS_contract_id"])].index)

    LS_Closing = pd.concat([LS_Closing,temp],axis=0)
    LS_Closing =LS_Closing.reset_index(drop=True)
    return LS_Closing,LS_Liquidation