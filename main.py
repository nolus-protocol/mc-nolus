import pandas as pd
from modules.LS_Opening import LS_Opening_Generate
from modules.MP_Asset_Daily import MP_Assets_Daily
from modules.PL_CurrentState import PL_State_ini
from modules.LP_Deposit import LP_Deposit_Generate
from modules.LS_Repayment import LS_Repayment_generate
from modules.LS_Liquidation import LS_Liquidation_generate
from modules.LP_Withdraw import LP_Withdraw_generate
from modules.LP_Pool import LP_pool_gen
from modules.LS_Closing import LS_Closing_ini
from modules.LP_Pool_State import LP_Pool_State_gen
from modules.TR_Profit import TR_Profit_ini
from modules.TR_State import TR_State_ini
from modules.TR_Rewards_Distribution import TR_Rewards_Distribution_ini
from modules.PL_CurrentState import PL_State_finalize
from modules.LP_Lender_CurrentState import LP_Lender_state_gen
from modules import LS_Interest
from modules.LS_State import LS_State_ini
from datetime import datetime, timedelta
import time
import json
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
matplotlib.use("TkAgg")


def client_distribution_generator(open_daily_count):
    dist = dict(open_daily_count)
    days = [eval(i) for i in dist.keys()]
    count = list(dist.values())
    distribution = pd.DataFrame({"Days": [], "Count": []})
    for i in range(0, len(days) - 1):
        #print(i)
        temp = pd.DataFrame({"Days": range(days[i], days[i + 1], 1)})
        temp.loc[temp["Days"] == days[i], "Count"] = count[i]
        temp.loc[temp["Days"] == days[i + 1], "Count"] = count[i + 1]
        distribution = pd.concat([distribution, temp], axis=0, ignore_index=True)
    distribution["Days"] = distribution["Days"].astype("uint64")
    distribution["Count"] = distribution["Count"].interpolate(method='linear').astype("uint64")
    return distribution


def add_hyperparameter(Name,Data):
    with open("config.json", 'r') as f:
        args = json.load(f)
    args[""+Name+""] = Data
    with open("config.json", 'w') as f:
        json.dump(args,f)
    return


def Monte_Carlo_simulation(startup_args):
    tic = time.perf_counter()
    for i in range(startup_args["MC_runs"]):
        args = startup_args
        #simulation
        #if args["mc_mode"] == "future":
        #    MP_Asset = pd.read_csv("MP_ASSET.csv", index_col=0)
        #    MP_Asset = prompt_future_distributions(MP_Asset, args)
        #else:
        MP_Asset, MP_Asset_State = MP_Assets_Daily(args)
        if i==0:
            nolus_price =MP_Asset.loc[MP_Asset["MP_asset_symbol"]==args["nolus_token_symbol"]].reset_index(drop=True)
            timestamps = nolus_price
            LP_Count_Closed, LP_Count_Open, LP_Interest, LP_Pool_Util_1, LP_Pool_Util_2, LP_Repayment, LS_Count_Closed, LS_Count_Open, LS_Interest_mc, LS_Repayment_mc, MC_Nolus_price, PL_Utilization, TR_Rewards, PL_TotBorStable, PL_TotDepStable, PL_TotValueLocked, TR_ProfitAmntStable, TR_ProfitAmntNls, TR_AmntStable, TR_AmntNls,TR_RewardsAmntStable,TR_RewardsAmntNls =mc_timestamp_ini(timestamps)
        min_timestamp = MP_Asset.loc[MP_Asset["MP_timestamp"] == min(MP_Asset["MP_timestamp"]),"MP_timestamp"][0]
        PL_State = PL_State_ini(MP_Asset)
        LS_Closing = LS_Closing_ini()
        LP_Pool = LP_pool_gen(args)
        LS_Opening = LS_Opening_Generate(MP_Asset, LP_Pool, args)
        LP_Deposit = LP_Deposit_Generate(MP_Asset, LP_Pool, args)
        LS_Repayment = LS_Repayment_generate(LS_Opening, LP_Pool, args)
        LS_Liquidation = LS_Liquidation_generate(MP_Asset, LS_Opening, LS_Repayment, args)
        LP_Deposit,LP_Withdraw, SYS_LP_Withdraw = LP_Withdraw_generate(LP_Deposit, LP_Pool, args)
        LP_Pool_State = LP_Pool_State_gen(LP_Pool,min_timestamp,args)
        TR_Profit = TR_Profit_ini()
        TR_Rewards_Distribution = TR_Rewards_Distribution_ini()
        TR_State = TR_State_ini(min_timestamp,args,nolus_price["MP_price_in_stable"][0])
        LS_State = LS_State_ini(LS_Opening, args)
        #nolus_price = pd.read_csv(args["nls_file_name"], index_col=0)
        #nolus_price["MP_timestamp"] = MP_Asset.drop_duplicates(subset="MP_timestamp")["MP_timestamp"]
        LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, LP_Pool_State, SYS_LP_Withdraw, PL_Interest, LS_Closing, PL_State, TR_State, TR_Profit, TR_Rewards_Distribution, LS_State, nolus_price = LS_Interest.MC_dayli_calculcations(MP_Asset, LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, SYS_LP_Withdraw,LP_Pool_State, LS_Closing, PL_State, TR_Profit, TR_State, TR_Rewards_Distribution, LS_State, nolus_price,LP_Pool, args)
        PL_State = PL_State_finalize(nolus_price, PL_State, LP_Pool_State, LS_Opening, LS_Repayment, LS_Closing,
                                     LP_Deposit, LP_Withdraw, TR_Profit,
                                     TR_Rewards_Distribution, PL_Interest, args)

        LP_Lender_State = SYS_LP_Withdraw[["LP_timestamp","LP_address_id","LP_Pool_id","LP_Lender_rewards_nls_total","LP_Lender_rewards_stable"]]
        #Part1
        zeros = pd.DataFrame(columns=LS_Liquidation.columns,
                             data=[[MP_Asset["MP_timestamp"].min(), 0, "tether", 0, 0, 0, 0, 0, 0, 1, "tether", 0, 0, 0],
                                   [MP_Asset["MP_timestamp"].min(), 0, "tether", 0, 0, 0, 0, 0, 0, 2, "tether", 0, 0, 0],
                                   [MP_Asset["MP_timestamp"].min(), 0, "tether", 0, 0, 0, 0, 0, 0, 3, "tether", 0, 0, 0]])
        LS_Liquidation = pd.concat([LS_Liquidation, zeros], axis=0).reset_index(drop=True)
        PL_Utilization["" + str(i) + ""] = PL_Interest[["PL_timestamp", "Util"]].groupby("PL_timestamp").mean().reset_index(drop=True)["Util"].values
        # hardcoded .... FOR NOW
        LP_Pool_Util_1["" + str(i) + ""] = PL_Interest.loc[PL_Interest["LP_Pool_id"] == "pid100", ["Util"]].reset_index(
            drop=True)
        LP_Pool_Util_2["" + str(i) + ""] = PL_Interest.loc[PL_Interest["LP_Pool_id"] == "pid101", ["Util"]].reset_index(
            drop=True)

        LS_Count_Open["" + str(i) + ""] = PL_State["PL_LS_count_open"]
        LS_Count_Closed["" + str(i) + ""] = PL_State["PL_LS_count_closed"]
        LP_Count_Open["" + str(i) + ""] = PL_State["PL_LP_count_open"]
        LP_Count_Closed["" + str(i) + ""] = PL_State["PL_LP_count_closed"]

        LS_Interest_mc["" + str(i) + ""] = \
        PL_Interest[["PL_timestamp", "LS_interest"]].groupby("PL_timestamp").mean().reset_index(drop=True)[
            "LS_interest"].values * 100  # in percents
        LP_Interest["" + str(i) + ""] = \
        PL_Interest[["PL_timestamp", "LP_interest"]].groupby("PL_timestamp").mean().reset_index(drop=True)[
            "LP_interest"].values * 100  # in percents

        TR_Rewards["" + str(i) + ""] = \
        TR_Rewards_Distribution[["TR_Rewards_timestamp", "TR_Rewards_amnt_stable"]].groupby(
            "TR_Rewards_timestamp").sum().reset_index(drop=True)["TR_Rewards_amnt_stable"].values

        a = LS_Repayment[["LS_timestamp", "LS_amnt_stable"]].groupby(
            "LS_timestamp").sum().reset_index(inplace=False)
        a["LS_timestamp"] = a["LS_timestamp"].astype("str")
        LS_Repayment_mc[""+str(i)+""] = LS_Repayment_mc["timestamp"].map(dict(a[["LS_timestamp", "LS_amnt_stable"]].values)).fillna(0)
        a = LP_Withdraw[["LP_timestamp", "LP_amnt_stable"]].groupby("LP_timestamp").sum().reset_index(inplace=False)
        a["LP_timestamp"] = a["LP_timestamp"].astype("str")
        LP_Repayment["" + str(i) + ""] = LP_Repayment["timestamp"].map(dict(a.values)).fillna(0)
        MC_Nolus_price["" + str(i) + ""] = nolus_price["MP_price_in_stable"]
        # addition for MC
        tbtdtvl = LP_Pool_State[["LP_Pool_timestamp", "SYS_LP_Pool_TV_IntDep_stable","LP_Pool_total_borrowed_stable","LP_Pool_total_deposited_stable"]]
        tbtdtvl.groupby("LP_Pool_timestamp").sum().reset_index(inplace=False)
        PL_TotBorStable["" + str(i) + ""] = PL_TotBorStable["timestamp"].map(dict(tbtdtvl[["LP_Pool_timestamp","LP_Pool_total_borrowed_stable"]].values)).fillna(0)
        PL_TotDepStable["" + str(i) + ""] = PL_TotDepStable["timestamp"].map(dict(tbtdtvl[["LP_Pool_timestamp","LP_Pool_total_deposited_stable"]].values)).fillna(0)
        PL_TotValueLocked["" + str(i) + ""] = PL_TotValueLocked["timestamp"].map(dict(tbtdtvl[["LP_Pool_timestamp","SYS_LP_Pool_TV_IntDep_stable"]].values)).fillna(0)
        prfts = TR_Profit[["TR_Profit_timestamp","TR_Profit_amnt_stable","TR_Profit_amnt_nls"]]
        TR_ProfitAmntNls["" + str(i) + ""] = TR_ProfitAmntNls["timestamp"].map(dict(prfts[["TR_Profit_timestamp","TR_Profit_amnt_nls"]].values)).fillna(0)
        TR_ProfitAmntStable["" + str(i) + ""] = TR_ProfitAmntStable["timestamp"].map(dict(prfts[["TR_Profit_timestamp","TR_Profit_amnt_stable"]].values)).fillna(0)
        TR_AmntStable["" + str(i) + ""] = TR_AmntStable["timestamp"].map(dict(TR_State[["TR_timestamp","TR_amnt_stable"]].values)).fillna(0)
        TR_AmntNls["" + str(i) + ""] = TR_AmntNls["timestamp"].map(dict(TR_State[["TR_timestamp","TR_amnt_nls"]].values)).fillna(0)
        a = TR_Rewards_Distribution.groupby("TR_Rewards_timestamp").sum().reset_index(inplace=False)
        a = a[["TR_Rewards_timestamp","TR_Rewards_amnt_stable","TR_Rewards_amnt_nls"]]
        TR_RewardsAmntNls["" + str(i) + ""] = TR_RewardsAmntNls["timestamp"].map(dict(a[["TR_Rewards_timestamp","TR_Rewards_amnt_nls"]].values)).fillna(0)
        TR_RewardsAmntStable["" + str(i) + ""] = TR_RewardsAmntStable["timestamp"].map(dict(a[["TR_Rewards_timestamp","TR_Rewards_amnt_stable"]].values)).fillna(0)
        startup_args["seed"] = args["seed"]
        if i==0:
            if True:
                PL_Interest.to_csv("PBI/PL_Interest_analisys.csv", index=False)
                LS_State.to_csv("PBI/LS_State_analisys.csv", index=False)
                LS_Opening.to_csv("PBI/LS_Opening_analisys.csv", index=False)
                LP_Deposit.to_csv("PBI/LP_Deposit_analisys.csv", index=False)
                SYS_LP_Withdraw.to_csv("PBI/SYS_LP_Withdraw_analisys.csv", index=False)
                LP_Withdraw.to_csv("PBI/LP_Withdraw_analisys.csv", index=False)

                LS_Repayment.to_csv("PBI/LS_Repayment_analisys.csv", index=False)
                LS_Liquidation.to_csv("PBI/LS_Liquidation_analisys.csv", index=False)
                LP_Pool_State.to_csv("PBI/LP_Pool_State_analisys.csv", index=False)
                TR_Profit.to_csv("PBI/TR_Profit_analisys.csv", index=False)
                PL_State.to_csv("PBI/PL_State_analisys.csv", index=False)
                LS_Closing.to_csv("PBI/LS_Closing_analisys.csv", index=False)
                TR_State.to_csv("PBI/TR_State_analisys.csv", index=False)
                LP_Lender_State.to_csv("PBI/LP_Lender_State_analisys.csv", index=False)
                TR_Rewards_Distribution.to_csv("PBI/TR_Rewards_Distribution_analisys.csv", index=False)
                MP_Asset = pd.concat([MP_Asset, nolus_price], axis=0)
                MP_Asset.to_csv("PBI/MP_Asset_final__analisys.csv", index=False)
            if False:
                PL_Interest.to_csv("PBI/PL_Interest.csv", index=False)
                LS_State.to_csv("PBI/LS_State.csv", index=False)
                LS_Opening.to_csv("PBI/LS_Opening.csv", index=False)
                LP_Deposit.to_csv("PBI/LP_Deposit.csv", index=False)
                SYS_LP_Withdraw.to_csv("PBI/SYS_LP_Withdraw.csv", index=False)
                LP_Withdraw.to_csv("PBI/LP_Withdraw.csv", index=False)

                LS_Repayment.to_csv("PBI/LS_Repayment.csv", index=False)
                LS_Liquidation.to_csv("PBI/LS_Liquidation.csv", index=False)
                LP_Pool_State.to_csv("PBI/LP_Pool_State.csv", index=False)
                TR_Profit.to_csv("PBI/TR_Profit.csv", index=False)
                PL_State.to_csv("PBI/PL_State.csv", index=False)
                LS_Closing.to_csv("PBI/LS_Closing.csv", index=False)
                TR_State.to_csv("PBI/TR_State.csv", index=False)
                LP_Lender_State.to_csv("PBI/LP_Lender_State.csv", index=False)
                TR_Rewards_Distribution.to_csv("PBI/TR_Rewards_Distribution.csv", index=False)
                MP_Asset = pd.concat([MP_Asset,nolus_price],axis=0)
                MP_Asset.to_csv("PBI/MP_Asset_final.csv",index=False)
    all_table = pd.DataFrame({"timestamp":nolus_price["MP_timestamp"]})
    all_table["PL_Utilization_mean"] = PL_Utilization.mean(axis=1)
    all_table["PL_Utilization_min"] = PL_Utilization.min(axis=1)
    all_table["PL_Utilization_max"] = PL_Utilization.max(axis=1)
    all_table["LP_Pool_Util_1_mean"] = LP_Pool_Util_1.mean(axis=1)
    all_table["LP_Pool_Util_1_min"] = LP_Pool_Util_1.min(axis=1)
    all_table["LP_Pool_Util_1_max"] = LP_Pool_Util_1.max(axis=1)
    all_table["LP_Pool_Util_2_mean"] = LP_Pool_Util_2.mean(axis=1)
    all_table["LP_Pool_Util_2_min"] = LP_Pool_Util_2.min(axis=1)
    all_table["LP_Pool_Util_2_max"] = LP_Pool_Util_2.max(axis=1)
    all_table["LS_Count_Open_mean"] = LS_Count_Open.mean(axis=1)
    all_table["LS_Count_Open_min"] = LS_Count_Open.min(axis=1)
    all_table["LS_Count_Open_max"] = LS_Count_Open.max(axis=1)
    all_table["LP_Count_Open_mean"] = LP_Count_Open.mean(axis=1)
    all_table["LP_Count_Open_min"] = LP_Count_Open.min(axis=1)
    all_table["LP_Count_Open_max"] = LP_Count_Open.max(axis=1)
    all_table["LS_Count_Closed_mean"] = LS_Count_Closed.mean(axis=1)
    all_table["LS_Count_Closed_min"] = LS_Count_Closed.min(axis=1)
    all_table["LS_Count_Closed_max"] = LS_Count_Closed.max(axis=1)
    all_table["LP_Count_Closed_mean"] = LP_Count_Closed.mean(axis=1)
    all_table["LP_Count_Closed_min"] = LP_Count_Closed.min(axis=1)
    all_table["LP_Count_Closed_max"] = LP_Count_Closed.max(axis=1)
    all_table["LS_Interest_mean"] = LS_Interest_mc.mean(axis=1)
    all_table["LS_Interest_min"] = LS_Interest_mc.min(axis=1)
    all_table["LS_Interest_max"] = LS_Interest_mc.max(axis=1)
    all_table["LP_Interest_mean"] = LP_Interest.mean(axis=1)
    all_table["LP_Interest_min"] = LP_Interest.min(axis=1)
    all_table["LP_Interest_max"] = LP_Interest.max(axis=1)
    all_table["TR_Rewards_mean"] = TR_Rewards.mean(axis=1)
    all_table["TR_Rewards_min"] = TR_Rewards.min(axis=1)
    all_table["TR_Rewards_max"] = TR_Rewards.max(axis=1)
    all_table["LS_Repayment_mean"] = LS_Repayment_mc.mean(axis=1)
    all_table["LS_Repayment_min"] = LS_Repayment_mc.min(axis=1)
    all_table["LS_Repayment_max"] = LS_Repayment_mc.max(axis=1)
    all_table["LP_Repayment_mean"] = LP_Repayment.mean(axis=1)
    all_table["LP_Repayment_min"] = LP_Repayment.min(axis=1)
    all_table["LP_Repayment_max"] = LP_Repayment.max(axis=1)
    all_table["MC_Nolus_price_mean"] = MC_Nolus_price.mean(axis=1)
    all_table["MC_Nolus_price_min"] = MC_Nolus_price.min(axis=1)
    all_table["MC_Nolus_price_max"] = MC_Nolus_price.max(axis=1)
    all_table["PL_TotalValueLocked_mean"] = PL_TotValueLocked.mean(axis=1)
    all_table["PL_TotalValueLocked_min"] = PL_TotValueLocked.min(axis=1)
    all_table["PL_TotalValueLocked_max"] = PL_TotValueLocked.max(axis=1)
    #additions for MC
    all_table["PL_TotDepStable_mean"] = PL_TotDepStable.mean(axis=1)
    all_table["PL_TotDepStable_min"] = PL_TotDepStable.min(axis=1)
    all_table["PL_TotDepStable_max"] = PL_TotDepStable.max(axis=1)
    all_table["PL_TotBorStable_mean"] = PL_TotBorStable.mean(axis=1)
    all_table["PL_TotBorStable_min"] = PL_TotBorStable.min(axis=1)
    all_table["PL_TotBorStable_max"] = PL_TotBorStable.max(axis=1)
    all_table["TR_ProfitAmntStable_mean"] = TR_ProfitAmntStable.mean(axis=1)
    all_table["TR_ProfitAmntStable_min"] = TR_ProfitAmntStable.min(axis=1)
    all_table["TR_ProfitAmntStable_max"] = TR_ProfitAmntStable.max(axis=1)
    all_table["TR_ProfitAmntNls_mean"] = TR_ProfitAmntNls.mean(axis=1)
    all_table["TR_ProfitAmntNls_min"] = TR_ProfitAmntNls.min(axis=1)
    all_table["TR_ProfitAmntNls_max"] = TR_ProfitAmntNls.max(axis=1)
    all_table["TR_AmntStable_mean"] = TR_AmntStable.mean(axis=1)
    all_table["TR_AmntStable_min"] = TR_AmntStable.min(axis=1)
    all_table["TR_AmntStable_max"] = TR_AmntStable.max(axis=1)
    all_table["TR_AmntNls_mean"] = TR_AmntNls.mean(axis=1)
    all_table["TR_AmntNls_min"] = TR_AmntNls.min(axis=1)
    all_table["TR_AmntNls_max"] = TR_AmntNls.max(axis=1)
    all_table["TR_RewardsAmntStable_mean"] = TR_RewardsAmntStable.mean(axis=1)
    all_table["TR_RewardsAmntStable_min"] = TR_RewardsAmntStable.min(axis=1)
    all_table["TR_RewardsAmntStable_max"] = TR_RewardsAmntStable.max(axis=1)
    all_table["TR_RewardsAmntNls_mean"] = TR_RewardsAmntNls.mean(axis=1)
    all_table["TR_RewardsAmntNls_min"] = TR_RewardsAmntNls.min(axis=1)
    all_table["TR_RewardsAmntNls_max"] = TR_RewardsAmntNls.max(axis=1)

    all_table.to_csv("PBI_MC/MC_Output.csv",index=False)
    PL_Utilization.to_csv("PBI_MC/PL_Utilization.csv",index=False)
    LP_Pool_Util_1.to_csv("PBI_MC/LP_Pool_Util_1.csv",index=False)
    LP_Pool_Util_2.to_csv("PBI_MC/LP_Pool_Util_2.csv",index=False)
    LS_Count_Open.to_csv("PBI_MC/LS_Count_Open.csv",index=False)
    LP_Count_Open.to_csv("PBI_MC/LP_Count_Open.csv",index=False)
    LS_Count_Closed.to_csv("PBI_MC/LS_Count_Closed.csv",index=False)
    LP_Count_Closed.to_csv("PBI_MC/LP_Count_Closed.csv",index=False)
    LS_Interest_mc.to_csv("PBI_MC/LS_Interest.csv",index=False)
    LP_Interest.to_csv("PBI_MC/LP_Interest.csv",index=False)
    TR_Rewards.to_csv("PBI_MC/TR_Rewards.csv",index=False)
    LS_Repayment_mc.to_csv("PBI_MC/LS_Repayment.csv",index=False)
    LP_Repayment.to_csv("PBI_MC/LP_Repayment.csv",index=False)
    MC_Nolus_price.to_csv("PBI_MC/MC_Nolus_price.csv",index=False)
    #additions for MC
    # PL_TotValueLocked.to_csv("PBI_MC/PL_TVL.csv",index=False)
    # PL_TotBorStable.to_csv("PBI_MC/PL_TotBorStable.csv",index=False)
    # PL_TotDepStable.to_csv("PBI_MC/PL_TotDepStable.csv",index=False)
    # TR_ProfitAmntStable.to_csv("PBI_MC/TR_ProfitAmntStable.csv",index=False)
    # TR_ProfitAmntNls.to_csv("PBI_MC/TR_ProfitAmntNls.csv",index=False)
    # TR_AmntNls.to_csv("PBI_MC/TR_AmntNls.csv",index=False)
    # TR_AmntStable.to_csv("PBI_MC/TR_AmntStable.csv",index=False)
    # TR_RewardsAmntNls.to_csv("PBI_MC/TR_RewardsAmntNls.csv",index=False)
    # TR_RewardsAmntStable.to_csv("PBI_MC/TR_RewardsAmntStable.csv",index=False)


    #Part 2 aggregation
    toc = time.perf_counter()
    print(toc-tic)
    print("DONE FFS....")
    #LS_int_main()

    return
# Press the green button in the gutter to run the script.


def mc_timestamp_ini(timestamps):
    PL_Utilization = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    LP_Pool_Util_1 = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    LP_Pool_Util_2 = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    LS_Count_Open = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    LP_Count_Open = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    LS_Count_Closed = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    LP_Count_Closed = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    LS_Interest_mc = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    LP_Interest = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    TR_Rewards = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    LS_Repayment_mc = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    LP_Repayment = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    MC_Nolus_price = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    PL_TotBorStable = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    PL_TotDepStable = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    PL_TotValueLocked = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    TR_ProfitAmntStable = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    TR_ProfitAmntNls = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    TR_AmntStable = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    TR_AmntNls = pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    TR_RewardsAmntStable= pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    TR_RewardsAmntNls= pd.DataFrame({"timestamp": timestamps["MP_timestamp"]})
    return LP_Count_Closed, LP_Count_Open, LP_Interest, LP_Pool_Util_1, LP_Pool_Util_2, LP_Repayment, LS_Count_Closed, LS_Count_Open, LS_Interest_mc, LS_Repayment_mc, MC_Nolus_price, PL_Utilization, TR_Rewards, PL_TotBorStable, PL_TotDepStable, PL_TotValueLocked, TR_ProfitAmntStable, TR_ProfitAmntNls, TR_AmntStable, TR_AmntNls,TR_RewardsAmntStable,TR_RewardsAmntNls


# Press the green button in the gutter to run the script.
def prompt_startup_distributions(startup_args):
    prompt = ""
    params = startup_args["Open_Daily_Count_dict"]
    while prompt != "start":
        print("Current configuration:" + str(params))
        distribution = client_distribution_generator(params)
        distribution.plot(x="Days", y="Count")
        plt.show()
        while True:
            table = ""
            prompt = input("Choose distribution[LS,LP]:")
            if prompt == "end":
                prompt = input("Start simulation?(y/n)")
                if prompt == "y":
                    prompt = "start"
                    break
                else:
                    continue
            else:
                table = prompt
            params = {}
            while True:
                prompt = input("Day:")
                if prompt == "end":
                    break

                day = prompt
                prompt = input("Count:")
                if prompt == "end":
                    break
                count = prompt
                params[day] = float(count)
            distribution = client_distribution_generator(params)
            distribution.plot(x="Days", y="Count")
            plt.show()
            prompt = input("Proceed updating " + table + " distribution?(y/n/)")
            if prompt == 'y':
                distribution.to_csv(str(startup_args["new_" + table + "_opened_daily_count"]))
            else:
                prompt = input("Start simulation?(y/n)")
                if prompt == 'y':
                    prompt = "start"
                    break
                else:
                    pass
    return

def gbm_simulation_positive(S0, mu, sigma, T, N):
    dt = T / N
    t = np.linspace(0, T, N+1)
    W = np.random.standard_normal(size=N+1)
    W = np.cumsum(W) * np.sqrt(dt)
    drift = (mu - 0.5 * sigma**2) * t
    diffusion = sigma * W
    S = S0 * np.exp(drift + diffusion)
    return S

def gbm_simulation_negative(S0, mu, sigma, T, N):
    dt = T / N
    t = np.linspace(0, T, N+1)
    W = np.random.standard_normal(size=N+1)
    W = np.cumsum(W) * np.sqrt(dt)
    drift = (mu - 0.5 * sigma**2) * t
    diffusion = sigma * W
    S = S0 * np.exp(drift - diffusion)
    return S


def prompt_future_distributions(MP_Asset,args,seed=None):
    if seed == None:
        seed = args["seed"]

    asset = MP_Asset
    asset_list = asset.drop_duplicates(subset="MP_asset_symbol")["MP_asset_symbol"].loc[~asset["MP_asset_symbol"].isin(args["Pool_Assets"])].tolist()
    desired_growth = float(args["future_percent_growth"]) / 100
    desired_period = float(args["future_interval"])
    dayli_growth_rate = (1 + desired_growth) ** (1 / desired_period) - 1
    Annual_growth_rate = ((1 + dayli_growth_rate) ** 365 - 1) / 100

    mu = Annual_growth_rate
    sigma = args["future_volatility"]
    T = 1  #time horizon in years
    N = args["future_interval"]
    np.random.seed(seed)
    for a in asset_list:
        work_asset = asset.loc[asset["MP_asset_symbol"] == a]
        asset = asset.drop(work_asset.index, axis=0)
        S0 = float(work_asset.loc[work_asset["MP_timestamp"] == work_asset["MP_timestamp"].max(), "MP_price_in_stable"])
        if args["future_percent_growth"] >= 0:
            prices = gbm_simulation_positive(S0, mu, sigma, T, N)
            while min(prices[1:]) < prices[0]:
                prices = gbm_simulation_positive(S0, mu, sigma, T, N)

        else:
            prices = gbm_simulation_negative(S0, mu, sigma, T, N)
            while max(prices[1:]) > prices[0]:
                prices = gbm_simulation_negative(S0, mu, sigma, T, N)

        next_date = pd.to_datetime(work_asset["MP_timestamp"].max()) + timedelta(days=int(args["future_interval"]))
        next_date = pd.date_range(start=work_asset["MP_timestamp"].max(), end=datetime.strftime(next_date,format="%Y-%m-%d"))
        next_date = next_date.astype("str")
        next_record = pd.DataFrame({"MP_timestamp":next_date,"MP_asset_symbol":a,"MP_price_in_stable":prices})
        work_asset = pd.concat([work_asset, next_record], axis=0).reset_index(drop=True)
        asset = pd.concat([asset, work_asset], axis=0).reset_index(drop=True)
    mu = 0
    sigma = 0.005
    T = 1  #time horizon in years
    N = args["future_interval"]

    stable_asset_list = MP_Asset.drop_duplicates(subset="MP_asset_symbol")["MP_asset_symbol"].loc[MP_Asset["MP_asset_symbol"].isin(args["Pool_Assets"])].tolist()
    for a in stable_asset_list:
        work_asset = asset.loc[asset["MP_asset_symbol"] == a]
        asset = asset.drop(work_asset.index, axis=0)
        S0 = float(work_asset.loc[work_asset["MP_timestamp"] == work_asset["MP_timestamp"].max(), "MP_price_in_stable"])
        if args["future_percent_growth"] >= 0:
            prices = gbm_simulation_positive(S0, mu, sigma, T, N)
        else:
            prices = gbm_simulation_negative(S0, mu, sigma, T, N)
        next_date = pd.to_datetime(work_asset["MP_timestamp"].max()) + timedelta(days=int(args["future_interval"]))
        next_date = pd.date_range(start=work_asset["MP_timestamp"].max(), end=datetime.strftime(next_date,format="%Y-%m-%d"))
        next_date = next_date.astype("str")
        next_record = pd.DataFrame({"MP_timestamp":next_date,"MP_asset_symbol":a,"MP_price_in_stable":prices})
        work_asset = pd.concat([work_asset, next_record], axis=0).reset_index(drop=True)
        asset = pd.concat([asset, work_asset], axis=0).reset_index(drop=True)
    timestamps_to_generate = work_asset.drop_duplicates(subset="MP_timestamp")["MP_timestamp"]
    MP_Asset = asset
    return MP_Asset,timestamps_to_generate

if __name__ == '__main__':
    with open("config.json", 'r') as f:
        startup_args = json.load(f)
    prompt_startup_distributions(startup_args)
    #todo: check if future flag is up and update MP_Asset and LS_tf.csv/ LP_tf.csv, with regards to the changes by LS/LP real_data aquired
    Monte_Carlo_simulation(startup_args)

    #TODO: decide how the simulation is about to run !!!!
    #MP_Asset = pd.read_csv("MP_ASSET.csv",index_col=0)
    #MP_Asset = prompt_future_distributions(MP_Asset,startup_args)
    #todo LS_Opening
    #todo: LS_Repayment, LS_Liquidation
    #todo: LP_Deposit
    #todo: LS_State
    #todo: LP_Lender_State



# See PyCharm help at https://www.jetbrains.com/help/pycharm/