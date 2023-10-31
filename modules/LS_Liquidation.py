import pandas as pd
def LS_Liquidation_generate(MP_Asset,LS_Opening,LS_Repayment,args):#WRONG form it ONLY from customers with penalty!
    contracts = LS_Opening.loc[LS_Opening["SYS_LS_expected_penalty"]>=1]["LS_contract_id"]
    contr_symbol = LS_Opening[["LS_contract_id","LS_asset_symbol","LS_cltr_amnt_asset"]]
    LS_Liquidation = LS_Repayment.loc[LS_Repayment["LS_contract_id"].isin(contracts)]
    LS_Liquidation["LS_transaction_type"] = 1
    LS_Liquidation=LS_Liquidation.drop(["LS_repayment_height","LS_repayment_idx"],axis=1)
    lp_dep_h = [s for s in range(100,100+len(LS_Liquidation))]
    lp_dep_idx = lp_dep_h
    LS_Liquidation["LS_timestamp"] = LS_Liquidation["LS_timestamp"].astype(str)
    #m = MP_Asset.rename(columns={"MP_asset_symbol":"SYS_LS_asset_symbol","MP_timestamp":"LS_timestamp","MP_price_in_stable":"SYS_LS_cltr_price"})
    LS_Liquidation = pd.merge(LS_Liquidation,contr_symbol,on="LS_contract_id",how="left")
    LS_Liquidation = LS_Liquidation.drop(columns={"LS_cltr_amnt_asset"},axis=1)
    #LS_Liquidation = pd.merge(LS_Liquidation,m, on=["LS_timestamp","SYS_LS_asset_symbol"],how="left")
    LS_Liquidation["LS_liquidation_height"] = lp_dep_h
    LS_Liquidation["LS_liquidation_idx"] = lp_dep_idx
    LS_Liquidation["LS_amnt_stable"] = 0
    LS_Liquidation["LS_principal_stable"] = 0
    LS_Liquidation["LS_current_margin_stable"] = 0
    LS_Liquidation["LS_current_interest_stable"] = 0
    LS_Liquidation["LS_prev_interest_stable"] = 0
    LS_Liquidation["LS_prev_margin_stable"] = 0


    return LS_Liquidation