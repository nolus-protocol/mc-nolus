import pandas as pd
import json

def LP_Lender_state_gen(MP_Asset, SYS_LP_Withdraw, TR_Rewards_Distribution, pool_id, args):


    LP_Lender_State = pd.DataFrame()
    LP_Lender_State["LP_Lender_id"] = SYS_LP_Withdraw["LP_address_id"]
    LP_Lender_State["LP_Pool_id"] = SYS_LP_Withdraw["LP_Pool_id"]
    LP_Lender_State["LP_timestamp"] = SYS_LP_Withdraw["LP_timestamp"]
    # create separate lender stable without interest for calculating rewards
    LP_Lender_State["SYS_LP_Lender_stable"] = SYS_LP_Withdraw["LP_amnt_stable"]
    LP_Lender_State["LP_Lender_stable"] = SYS_LP_Withdraw["LP_amnt_stable"] + SYS_LP_Withdraw["LP_interest_amnt"]

    # take amounts of deposited per timestamp
    contracts_total_by_timestamp = SYS_LP_Withdraw[["LP_timestamp", "LP_Pool_id", "LP_amnt_stable"]].groupby(
        ["LP_timestamp", "LP_Pool_id"]).sum().reset_index()
    contracts_total_by_timestamp = contracts_total_by_timestamp.rename(
        columns={"LP_amnt_stable": "SYS_LP_total_deposited"})
    # merge with lender_state
    LP_Lender_State = pd.merge(LP_Lender_State, contracts_total_by_timestamp, on=["LP_timestamp", "LP_Pool_id"],
                               how='left')
    # calculate percentage of daily reawards per lender id
    LP_Lender_State["SYS_percent_rewards"] = LP_Lender_State["SYS_LP_Lender_stable"] / LP_Lender_State[
        "SYS_LP_total_deposited"]
    rewards = TR_Rewards_Distribution[
        ["TR_Rewards_timestamp", "TR_Rewards_Pool_id", "TR_Rewards_amnt_stable", "TR_Rewards_amnt_nls"]]
    rewards = rewards.rename(columns={"TR_Rewards_timestamp": "LP_timestamp", "TR_Rewards_Pool_id": "LP_Pool_id"})
    rewards["LP_timestamp"] = rewards["LP_timestamp"].astype('datetime64[ns]')
    LP_Lender_State = pd.merge(LP_Lender_State, rewards, on=["LP_timestamp", "LP_Pool_id"],how='left')
    # distribute rewards in stable and nls by multiplying values of tr_rewards with the SYS_percent_rewards
    LP_Lender_State["SYS_LP_Lender_reward_stable"] = LP_Lender_State["TR_Rewards_amnt_stable"] * LP_Lender_State[
        "SYS_percent_rewards"]
    LP_Lender_State["SYS_LP_Lender_reward_nls"] = LP_Lender_State["TR_Rewards_amnt_nls"] * LP_Lender_State[
        "SYS_percent_rewards"]

    c = MP_Asset.loc[MP_Asset["MP_asset_symbol"].isin(args["Pool_Assets"])]
    c["LP_Pool_id"] = c["MP_asset_symbol"].map(dict(pool_id[["LP_symbol", "LP_Pool_id"]].values))
    y = c.loc[c["MP_asset_symbol"] != args["currency_stable"]]
    f = c.loc[c["MP_asset_symbol"] == args["currency_stable"], ["MP_timestamp", "MP_price_in_stable"]]
    y["stable_price"] = y["MP_timestamp"].map(dict(f.values))
    y["MP_price_in_stable"] = y["MP_price_in_stable"] / y["stable_price"]
    y = y[["MP_timestamp", "LP_Pool_id", "MP_price_in_stable"]]
    y = y.rename(columns={"MP_timestamp": "LP_timestamp"})
    # multiply with const to get the amnt in pool_asset => fillna(1) where pool = currency_stable
    y["LP_timestamp"] = y["LP_timestamp"].astype('datetime64[ns]')
    LP_Lender_State = pd.merge(LP_Lender_State, y, on=["LP_timestamp", "LP_Pool_id"], how="left").fillna(1)
    LP_Lender_State["LP_Lender_asset"] = LP_Lender_State["LP_Lender_stable"] * LP_Lender_State["MP_price_in_stable"]
    LP_Lender_State["SYS_LP_Lender_reward_asset"] = LP_Lender_State["SYS_LP_Lender_reward_stable"] * LP_Lender_State[
        "MP_price_in_stable"]

    LP_Lender_State = LP_Lender_State.drop(
        ["MP_price_in_stable", "TR_Rewards_amnt_stable", "TR_Rewards_amnt_nls", "SYS_percent_rewards",
         "SYS_LP_Lender_stable", "SYS_LP_total_deposited"], axis=1)
    return LP_Lender_State