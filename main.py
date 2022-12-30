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


def start(startup_args):
    tic = time.perf_counter()
    nolus_ini_price = pd.read_csv("MP_Asset_nolus.csv")
    PL_Utilization = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LP_Pool_Util_1 = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LP_Pool_Util_2 = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LS_Count_Open = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LP_Count_Open = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LS_Count_Closed = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LP_Count_Closed = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LS_Interest_mc = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LP_Interest = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    TR_Rewards = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LS_Repayment_mc = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LP_Repayment = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    MC_Nolus_price = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    for i in range(startup_args["MC_runs"]):
        args = startup_args
        #simulation
        MP_Asset, MP_Asset_State = MP_Assets_Daily(args)
        min_timestamp = MP_Asset.loc[MP_Asset["MP_timestamp"] == min(MP_Asset["MP_timestamp"]),"MP_timestamp"][0]
        PL_State = PL_State_ini(MP_Asset)
        LS_Closing = LS_Closing_ini()
        LP_Pool = LP_pool_gen(args)
        LS_Opening = LS_Opening_Generate(MP_Asset, LP_Pool, args)
        LP_Deposit = LP_Deposit_Generate(MP_Asset, LP_Pool, args)
        LS_Repayment = LS_Repayment_generate(LS_Opening, LP_Pool, args)
        LS_Liquidation = LS_Liquidation_generate(MP_Asset, LS_Opening, LS_Repayment, args)
        LP_Withdraw, SYS_LP_Withdraw = LP_Withdraw_generate(LP_Deposit, LP_Pool, args)
        LP_Pool_State = LP_Pool_State_gen(LP_Pool,min_timestamp,args)
        TR_Profit = TR_Profit_ini()
        TR_Rewards_Distribution = TR_Rewards_Distribution_ini()
        TR_State = TR_State_ini()
        LS_State = LS_State_ini(LS_Opening, args)
        nolus_price = pd.read_csv(args["nls_file_name"], index_col=0)
        LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, LP_Pool_State, SYS_LP_Withdraw, PL_Interest, LS_Closing, PL_State, TR_State, TR_Profit, TR_Rewards_Distribution, LS_State, nolus_price = LS_Interest.MC_dayli_calculcations(MP_Asset, LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, SYS_LP_Withdraw,LP_Pool_State, LS_Closing, PL_State, TR_Profit, TR_State, TR_Rewards_Distribution, LS_State, nolus_price,LP_Pool, args)
        PL_State = PL_State_finalize(nolus_price, PL_State, LP_Pool_State, LS_Opening, LS_Repayment, LS_Closing,
                                     LP_Deposit, LP_Withdraw, TR_Profit,
                                     TR_Rewards_Distribution, PL_Interest, args)
        LP_Lender_State = LP_Lender_state_gen(MP_Asset, SYS_LP_Withdraw, TR_Rewards_Distribution, LP_Pool, args)
        LS_State =LS_State.drop(columns={"SYS_LS_staked_cltr_in_stable_left","coef","LS_principal_stable_left","borowed_to_cltr","post_to_pre"})
        #Part1

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

        LS_Repayment_mc["" + str(i) + ""] = LS_Repayment_mc["timestamp"].map(dict(
            LS_Repayment[["LS_timestamp", "LS_amnt_stable"]].groupby(
                "LS_timestamp").sum().reset_index().values)).fillna(0)
        a = LP_Withdraw[["LP_timestamp", "LP_amnt_stable"]]
        a = a.groupby("LP_timestamp").sum().reset_index()
        LP_Repayment["" + str(i) + ""] = LP_Repayment["timestamp"].map(dict(a.values)).fillna(0)
        MC_Nolus_price["" + str(i) + ""] = nolus_price["MP_price_in_stable"]

        startup_args["seed"] = args["seed"]
        if i==0:
            PL_Interest.to_csv("PL_Interest.csv", index=False)
            LS_State.to_csv("LS_State.csv", index=False)
            LS_Opening.to_csv("LS_Opening.csv", index=False)
            LP_Deposit.to_csv("LP_Deposit.csv", index=False)
            SYS_LP_Withdraw.to_csv("SYS_LP_Withdraw.csv", index=False)
            LP_Withdraw.to_csv("LP_Withdraw.csv", index=False)

            LS_Repayment.to_csv("LS_Repayment.csv", index=False)
            LS_Liquidation.to_csv("LS_Liquidation.csv", index=False)
            LP_Pool_State.to_csv("LP_Pool_State.csv", index=False)
            TR_Profit.to_csv("TR_Profit.csv", index=False)
            PL_State.to_csv("PL_State.csv", index=False)
            LS_Closing.to_csv("LS_Closing.csv", index=False)
            TR_State.to_csv("TR_State.csv", index=False)
            LP_Lender_State.to_csv("LP_Lender_State.csv", index=False)
            TR_Rewards_Distribution.to_csv("TR_Rewards_Distribution.csv", index=False)
            MP_Asset = pd.concat([MP_Asset,nolus_price],axis=0)
            MP_Asset.to_csv("MP_Asset_final.csv",index=False)
    all_table = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
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

    all_table.to_csv("MC_Output.csv",index=False)
    PL_Utilization.to_csv("PL_Utilization.csv",index=False)
    LP_Pool_Util_1.to_csv("LP_Pool_Util_1.csv",index=False)
    LP_Pool_Util_2.to_csv("LP_Pool_Util_2.csv",index=False)
    LS_Count_Open.to_csv("LS_Count_Open.csv",index=False)
    LP_Count_Open.to_csv("LP_Count_Open.csv",index=False)
    LS_Count_Closed.to_csv("LS_Count_Closed.csv",index=False)
    LP_Count_Closed.to_csv("LP_Count_Closed.csv",index=False)
    LS_Interest_mc.to_csv("LS_Interest.csv",index=False)
    LP_Interest.to_csv("LP_Interest.csv",index=False)
    TR_Rewards.to_csv("TR_Rewards.csv",index=False)
    LS_Repayment_mc.to_csv("LS_Repayment.csv",index=False)
    LP_Repayment.to_csv("LP_Repayment.csv",index=False)
    MC_Nolus_price.to_csv("MC_Nolus_price.csv",index=False)
 
    #Part 2 aggregation
    #todo: MP_Asset+ MC_Nolus_price
    toc = time.perf_counter()
    print(toc-tic)
    print("DONE FFS....")
    #LS_int_main()

    return
# Press the green button in the gutter to run the script.
def test():
    with open("config.json", 'r') as f:
        startup_args = json.load(f)
    nolus_ini_price = pd.read_csv("MP_Asset_nolus.csv")
    PL_Utilization = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LP_Pool_Util_1 = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LP_Pool_Util_2 = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LS_Count_Open = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LP_Count_Open = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LS_Count_Closed = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LP_Count_Closed = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LS_Interest_mc = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LP_Interest = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    TR_Rewards = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LS_Repayment_mc = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    LP_Repayment = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    MC_Nolus_price = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
    for i in range(2):
        args = startup_args
        PL_Interest = pd.read_csv("PL_Interest.csv")
        LP_Withdraw = pd.read_csv("LP_Withdraw.csv")
        LS_Repayment = pd.read_csv("LS_Repayment.csv")
        PL_State = pd.read_csv("PL_State.csv")
        TR_Rewards_Distribution = pd.read_csv("TR_Rewards_Distribution.csv", index_col=0)
        nolus_price = pd.read_csv("nolus_price_final.csv")

        PL_Utilization[""+str(i)+""] = PL_Interest[["PL_timestamp","Util"]].groupby("PL_timestamp").mean().reset_index(drop=True)["Util"].values
        #hardcoded .... FOR NOW
        LP_Pool_Util_1[""+str(i)+""] = PL_Interest.loc[PL_Interest["LP_Pool_id"]=="pid100",["Util"]].reset_index(drop=True)
        LP_Pool_Util_2[""+str(i)+""] = PL_Interest.loc[PL_Interest["LP_Pool_id"]=="pid101",["Util"]].reset_index(drop=True)

        LS_Count_Open[""+str(i)+""] = PL_State["PL_LS_count_open"]
        LS_Count_Closed[""+str(i)+""] = PL_State["PL_LS_count_closed"]
        LP_Count_Open[""+str(i)+""] = PL_State["PL_LP_count_open"]
        LP_Count_Closed[""+str(i)+""] = PL_State["PL_LP_count_closed"]

        LS_Interest_mc[""+str(i)+""] = PL_Interest[["PL_timestamp","LS_interest"]].groupby("PL_timestamp").mean().reset_index(drop=True)["LS_interest"].values*100#in percents
        LP_Interest[""+str(i)+""] = PL_Interest[["PL_timestamp","LP_interest"]].groupby("PL_timestamp").mean().reset_index(drop=True)["LP_interest"].values*100#in percents

        TR_Rewards[""+str(i)+""] = TR_Rewards_Distribution[["TR_Rewards_timestamp","TR_Rewards_amnt_stable"]].groupby("TR_Rewards_timestamp").sum().reset_index(drop=True)["TR_Rewards_amnt_stable"].values

        LS_Repayment_mc[""+str(i)+""]  = LS_Repayment_mc["timestamp"].map(dict(LS_Repayment[["LS_timestamp","LS_amnt_stable"]].groupby("LS_timestamp").sum().reset_index().values)).fillna(0)
        a= LP_Withdraw[["LP_timestamp","LP_amnt_stable"]]
        a = a.groupby("LP_timestamp").sum().reset_index()
        LP_Repayment["" + str(i) + ""] = LP_Repayment["timestamp"].map(dict(a.values)).fillna(0)
        MC_Nolus_price[""+str(i)+""] = nolus_price["MP_price_in_stable"]

        startup_args["seed"] = args["seed"]
    print(1)
    #Phase 2:
    all_table = pd.DataFrame({"timestamp":nolus_ini_price["MP_timestamp"]})
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

    all_table.to_csv("all_table.csv",index=False)
    return

if __name__ == '__main__':
    prompt = ""
    with open("config.json", 'r') as f:
        startup_args = json.load(f)
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
    #test()
    start(startup_args)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/