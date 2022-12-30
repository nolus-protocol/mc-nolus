import numpy as np
import pandas as pd

def TR_Rewards_Distribution_ini():
    TR_Rewards_Distribution = pd.DataFrame({"TR_Rewards_height":[],"TR_Rewards_idx":[],"TR_Rewards_Pool_id":[],"TR_Rewards_timestamp":[],"TR_Rewards_amnt_stable":[],"TR_Rewards_amnt_nls":[]})
    return TR_Rewards_Distribution

def TR_Rewards_Distribution_update(timestamp,nolus_price,TR_Rewards_Distribution,TR_State,LP_Pool_State,args):
    rewards_dist = pd.read_csv(args["tvl_rewards_csv"])
    a = pd.DataFrame(args["symbol_digit"])
    c = a.loc[a["symbol"] == args["currency_stable"]]["digit"].values[0].astype(float)
    rewards_dist["pool_tvl"] = rewards_dist["pool_tvl"] * 10 ** c
    pool_tvl = LP_Pool_State.loc[
        LP_Pool_State["LP_Pool_timestamp"] == timestamp, ["LP_Pool_id", "SYS_LP_Pool_TV_IntDep_stable"]]
    ptvl = pool_tvl.rename(columns={"SYS_LP_Pool_TV_IntDep_stable": "pool_tvl"})
    rewards_dist = pd.concat([rewards_dist, ptvl], axis=0)
    rewards_dist = rewards_dist.sort_values(by="pool_tvl", ignore_index=True)#.fillna(method=   )
    rewards_dist["nls_rewards"] = rewards_dist["nls_rewards"].interpolate(method="linear", limit_direction="both",                                                                      axis=0)
    rewards_dist = rewards_dist.drop(["pool_tvl"], axis=1)#check fillna(met)
    amnt = TR_State.loc[TR_State["TR_timestamp"]==timestamp,"TR_amnt_stable"].reset_index(drop=True)
    rewards_dist["pool_rewards"] = (rewards_dist["nls_rewards"]/365)*amnt.values[0]
    pool_rewards = rewards_dist[["LP_Pool_id","pool_rewards"]].dropna(axis=0)

    temp = pd.DataFrame({"TR_Rewards_height":np.ones(len(pool_rewards)),"TR_Rewards_idx":np.ones(len(pool_rewards)),"TR_Rewards_Pool_id":pool_rewards["LP_Pool_id"],"TR_Rewards_timestamp":timestamp,"TR_Rewards_amnt_stable":pool_rewards["pool_rewards"],"TR_Rewards_amnt_nls":pool_rewards["pool_rewards"]*nolus_price.loc[nolus_price["MP_timestamp"]==timestamp]["MP_price_in_stable"].values})#args["nolus_token_price_ini"]
    TR_Rewards_Distribution= TR_Rewards_Distribution.drop(TR_Rewards_Distribution.loc[TR_Rewards_Distribution["TR_Rewards_timestamp"] == timestamp].index)
    TR_Rewards_Distribution = pd.concat([TR_Rewards_Distribution, temp], axis=0, ignore_index=True)
    LP_Pool_State.loc[LP_Pool_State["LP_Pool_timestamp"]== timestamp,["SYS_LP_Pool_TV_IntDep_stable"]] = LP_Pool_State.loc[LP_Pool_State["LP_Pool_timestamp"]== timestamp,"SYS_LP_Pool_TV_IntDep_stable"] + LP_Pool_State.loc[LP_Pool_State["LP_Pool_timestamp"]== timestamp,"LP_Pool_id"].map(dict(pool_rewards.values)).fillna(0)
    return pool_rewards, TR_Rewards_Distribution