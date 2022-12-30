import pandas as pd

def LS_Closing_ini():
    LS_Closing = pd.DataFrame({"LS_timestamp":[],"LS_contract_id":[]})
    return LS_Closing

def LS_Closing_update(timestamp,LS_Closing,LS_Repayment,LS_Liquidation,LS_Opening):
    #select the last record in repayment for the given timestamp and create a closing record for it with all the data needed
    temp = LS_Repayment.drop_duplicates(subset=["LS_contract_id"],keep="last",ignore_index=True)
    temp = temp[["LS_timestamp","LS_contract_id"]]
    temp = temp.loc[temp["LS_timestamp"]==timestamp]
    temp2 = LS_Liquidation.loc[LS_Liquidation["LS_contract_id"].isin(temp["LS_contract_id"]),["LS_contract_id", "SYS_LS_cltr_amnt_taken"]]
    temp2 = temp2.groupby("LS_contract_id").sum().reset_index()
    temp3 = LS_Opening.loc[LS_Opening["LS_contract_id"].isin(temp["LS_contract_id"]),["LS_contract_id", "LS_cltr_amnt_asset"]]
    temp = pd.merge(temp,temp2, on="LS_contract_id", how='left')
    temp = pd.merge(temp,temp3,on="LS_contract_id", how='left')
    cltr_price = LS_Liquidation[["LS_contract_id","SYS_LS_cltr_price"]].drop_duplicates(subset=["LS_contract_id"],keep="last",ignore_index=True)
    temp = pd.merge(temp,cltr_price, on="LS_contract_id", how='left')
    temp = temp.fillna(0)

    LS_Closing = LS_Closing.drop(LS_Closing.loc[LS_Closing["LS_contract_id"].isin(temp["LS_contract_id"])].index)

    LS_Closing = pd.concat([LS_Closing,temp],axis=0,ignore_index=True)
    return LS_Closing

def LS_Closing_market_cond_update(timestamp, LS_Closing, LS_Liquidation, LS_Opening):
    temp = LS_Liquidation.loc[LS_Liquidation["LS_timestamp"]==timestamp]
    temp = temp.loc[temp["LS_transaction_type"]==2,["LS_timestamp","LS_contract_id", "SYS_LS_cltr_amnt_taken","SYS_LS_cltr_price"]]
    temp2 = LS_Opening.loc[LS_Opening["LS_contract_id"].isin(temp["LS_contract_id"]),["LS_contract_id", "LS_cltr_amnt_asset"]]
    temp = pd.merge(temp,temp2,on="LS_contract_id",how="left")

    LS_Closing = LS_Closing.drop(LS_Closing.loc[LS_Closing["LS_contract_id"].isin(temp["LS_contract_id"])].index)

    LS_Closing = pd.concat([LS_Closing,temp],axis=0,ignore_index=True)
    return LS_Closing