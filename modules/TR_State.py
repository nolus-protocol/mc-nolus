import pandas as pd


def TR_State_ini():
    TR_State = pd.DataFrame({"TR_timestamp":[],"TR_amnt_stable":[],"TR_amnt_nls":[]})
    return TR_State

def TR_State_update(timestamp,nolus_price,prev_timestamp,TR_Profit,TR_State,args):
    if prev_timestamp == None:
        amnt_stable = TR_Profit.loc[TR_Profit["TR_Profit_timestamp"]==timestamp,"TR_Profit_amnt_stable"] #+ TR_Profit.loc[TR_Profit["TR_Profit_timestamp"]==prev_timestamp,"TR_Profit_amnt_stable"].values()
    else:
        amnt_stable = TR_Profit.loc[TR_Profit["TR_Profit_timestamp"]==timestamp,"TR_Profit_amnt_stable"].values[0] + TR_State.loc[TR_State["TR_timestamp"]==prev_timestamp,"TR_amnt_stable"].values[0]
    temp = pd.DataFrame({"TR_timestamp":timestamp,"TR_amnt_stable":amnt_stable,"TR_amnt_nls":amnt_stable*nolus_price.loc[nolus_price["MP_timestamp"]==timestamp, "MP_price_in_stable"].values},index=[0])#args["nolus_token_price_ini"]
    TR_State = TR_State.drop(TR_State.loc[TR_State["TR_timestamp"] == timestamp].index)
    TR_State = pd.concat([TR_State, temp], axis=0, ignore_index=True)
    return TR_State
