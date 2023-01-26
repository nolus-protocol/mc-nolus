import pandas as pd


def TR_State_ini(timestamp,args,nls):
    TR_State = pd.DataFrame({"TR_timestamp":[timestamp],"TR_amnt_stable":[args["nolus_token_count_ini"]*nls],"TR_amnt_nls":[args["nolus_token_count_ini"]]})
    return TR_State

def TR_State_update(timestamp,nolus_price,prev_timestamp,TR_Profit,TR_State,args):
    amnt_nls = TR_Profit.loc[TR_Profit["TR_Profit_timestamp"].astype("str")==timestamp,"TR_Profit_amnt_nls"].values[0] + TR_State.loc[TR_State["TR_timestamp"]==prev_timestamp,"TR_amnt_nls"].values[0]
    temp = pd.DataFrame({"TR_timestamp":timestamp,"TR_amnt_stable":amnt_nls*nolus_price.loc[nolus_price["MP_timestamp"]==timestamp, "MP_price_in_stable"].values,"TR_amnt_nls":amnt_nls},index=[0])#args["nolus_token_price_ini"]
    TR_State = TR_State.drop(TR_State.loc[TR_State["TR_timestamp"] == timestamp].index)
    TR_State = pd.concat([TR_State, temp], axis=0, ignore_index=True)
    return TR_State
