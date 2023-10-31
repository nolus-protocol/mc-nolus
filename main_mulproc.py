import random

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
import concurrent.futures
from concurrent.futures import ALL_COMPLETED
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
        print(i)
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
    print("Process "+str(startup_args["run_number"])+" started!")
    i = startup_args["run_number"]
    args = startup_args
    # simulation
    MP_Asset, MP_Asset_State = MP_Assets_Daily(args)
    min_timestamp = MP_Asset.loc[MP_Asset["MP_timestamp"] == min(MP_Asset["MP_timestamp"]), "MP_timestamp"][0]
    nolus_price = pd.read_csv(args["nls_file_name"], index_col=0)
    nolus_price["MP_timestamp"] = MP_Asset.drop_duplicates(subset="MP_timestamp")["MP_timestamp"]
    tic = time.perf_counter()
    PL_State = PL_State_ini(MP_Asset)
    LS_Closing = LS_Closing_ini()
    LP_Pool = LP_pool_gen(args)
    LS_Opening = LS_Opening_Generate(MP_Asset, LP_Pool, args)
    LP_Deposit = LP_Deposit_Generate(MP_Asset, LP_Pool, args)
    LS_Repayment = LS_Repayment_generate(LS_Opening, LP_Pool, args)
    LS_Liquidation = LS_Liquidation_generate(MP_Asset, LS_Opening, LS_Repayment, args)
    LP_Deposit, LP_Withdraw, SYS_LP_Withdraw = LP_Withdraw_generate(LP_Deposit, LP_Pool, args)
    LP_Pool_State = LP_Pool_State_gen(LP_Pool, min_timestamp, args)
    TR_Profit = TR_Profit_ini()
    TR_Rewards_Distribution = TR_Rewards_Distribution_ini()
    TR_State = TR_State_ini(min_timestamp, args, nolus_price["MP_price_in_stable"][0])
    LS_State = LS_State_ini(LS_Opening, args)

    LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, LP_Pool_State, SYS_LP_Withdraw, PL_Interest, LS_Closing, PL_State, TR_State, TR_Profit, TR_Rewards_Distribution, LS_State, nolus_price = LS_Interest.MC_dayli_calculcations(
        MP_Asset, LS_Opening, LP_Deposit, LS_Repayment, LS_Liquidation, LP_Withdraw, SYS_LP_Withdraw, LP_Pool_State,
        LS_Closing, PL_State, TR_Profit, TR_State, TR_Rewards_Distribution, LS_State, nolus_price, LP_Pool, args)
    PL_State = PL_State_finalize(nolus_price, PL_State, LP_Pool_State, LS_Opening, LS_Repayment, LS_Closing,
                                 LP_Deposit, LP_Withdraw, TR_Profit,
                                 TR_Rewards_Distribution, PL_Interest, args)
    LP_Lender_State = SYS_LP_Withdraw[["LP_timestamp", "LP_address_id", "LP_Pool_id", "LP_Lender_rewards_nls_total", "LP_Lender_rewards_stable"]]

    nolus_ini_price = nolus_price
    PL_Utilization = pd.DataFrame({"timestamp": nolus_ini_price["MP_timestamp"]})
    LP_Pool_Util_1 = pd.DataFrame({"timestamp": nolus_ini_price["MP_timestamp"]})
    LP_Pool_Util_2 = pd.DataFrame({"timestamp": nolus_ini_price["MP_timestamp"]})
    LS_Count_Open = pd.DataFrame({"timestamp": nolus_ini_price["MP_timestamp"]})
    LP_Count_Open = pd.DataFrame({"timestamp": nolus_ini_price["MP_timestamp"]})
    LS_Count_Closed = pd.DataFrame({"timestamp": nolus_ini_price["MP_timestamp"]})
    LP_Count_Closed = pd.DataFrame({"timestamp": nolus_ini_price["MP_timestamp"]})
    LS_Interest_mc = pd.DataFrame({"timestamp": nolus_ini_price["MP_timestamp"]})
    LP_Interest = pd.DataFrame({"timestamp": nolus_ini_price["MP_timestamp"]})
    TR_Rewards = pd.DataFrame({"timestamp": nolus_ini_price["MP_timestamp"]})
    LS_Repayment_mc = pd.DataFrame({"timestamp": nolus_ini_price["MP_timestamp"]})
    LP_Repayment = pd.DataFrame({"timestamp": nolus_ini_price["MP_timestamp"]})
    MC_Nolus_price = pd.DataFrame({"timestamp": nolus_ini_price["MP_timestamp"]})

    PL_Utilization["" + str(i) + ""] = \
    PL_Interest[["PL_timestamp", "Util"]].groupby("PL_timestamp").mean().reset_index(drop=True)["Util"].values
    # hardcoded .... FOR NOW
    LP_Pool_Util_1["" + str(i) + ""] = PL_Interest.loc[PL_Interest["LP_Pool_id"] == "pid100", ["Util"]].reset_index(
        drop=True)
    LP_Pool_Util_2["" + str(i) + ""] = PL_Interest.loc[PL_Interest["LP_Pool_id"] == "pid101", ["Util"]].reset_index(
        drop=True)

    LS_Count_Open["" + str(i) + ""] = PL_State["PL_LS_count_open"].multiply(args["contract_weight"])
    LS_Count_Closed["" + str(i) + ""] = PL_State["PL_LS_count_closed"].multiply(args["contract_weight"])
    LP_Count_Open["" + str(i) + ""] = PL_State["PL_LP_count_open"].multiply(args["contract_weight"])
    LP_Count_Closed["" + str(i) + ""] = PL_State["PL_LP_count_closed"].multiply(args["contract_weight"])

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
    LS_Repayment_mc["" + str(i) + ""] = LS_Repayment_mc["timestamp"].map(dict(a[["LS_timestamp", "LS_amnt_stable"]].values)).fillna(0)
    a = LP_Withdraw[["LP_timestamp", "LP_amnt_stable"]].groupby("LP_timestamp").sum().reset_index(inplace=False)
    a["LP_timestamp"] = a["LP_timestamp"].astype("str")
    LP_Repayment["" + str(i) + ""] = LP_Repayment["timestamp"].map(dict(a.values)).fillna(0)
    MC_Nolus_price["" + str(i) + ""] = nolus_price["MP_price_in_stable"]
    print("Process "+str(startup_args["run_number"])+" ended!")
    if startup_args["run_number"] == 5:
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
        MP_Asset = pd.concat([MP_Asset, nolus_price], axis=0)
        MP_Asset.to_csv("MP_Asset_final.csv", index=False)
    return PL_Utilization,LP_Pool_Util_1,LP_Pool_Util_2,LS_Count_Open,LS_Count_Closed,LP_Count_Open,LP_Count_Closed,LS_Interest_mc,LP_Interest,TR_Rewards,LS_Repayment_mc,LP_Repayment,MC_Nolus_price
# Press the green button in the gutter to run the script.

if __name__ == '__main__':
    #test()
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
    tic = time.perf_counter()
    process_list = []
    result = []
    list_of_startup_args = []
    PL_Utilization = pd.DataFrame()
    LP_Pool_Util_1 = pd.DataFrame()
    LP_Pool_Util_2 = pd.DataFrame()
    LS_Count_Open = pd.DataFrame()
    LS_Count_Closed = pd.DataFrame()
    LP_Count_Open = pd.DataFrame()
    LP_Count_Closed = pd.DataFrame()
    LS_Interest_mc = pd.DataFrame()
    LP_Interest = pd.DataFrame()
    TR_Rewards = pd.DataFrame()
    LS_Repayment_mc = pd.DataFrame()
    LP_Repayment = pd.DataFrame()
    MC_Nolus_price = pd.DataFrame()
    process_count = 0
    for i in range(startup_args["MC_runs"]):
        startup_args["run_number"] = i
        list_of_startup_args.append(startup_args.copy())
        random.seed(startup_args["seed"])
        startup_args["seed"] = startup_args["seed"] + random.randint(30000, 50000)
        LS_Opening_analisys = pd.DataFrame()
    #process start - starts multiple processes (based on cpu cores 1 process per core), automatically release resources on process completion and starts a new one for len(list_of_startup_args)
    with concurrent.futures.ProcessPoolExecutor(6) as executor:
        print('MC_Processes : starting processes\n')
        print("Number of executions : " + str(startup_args["MC_runs"]))
        results = [executor.submit(start, arg) for arg in list_of_startup_args]
        concurrent.futures.wait(results,return_when=ALL_COMPLETED)
        for result in concurrent.futures.as_completed(results):
            try:
                result = result.result()
                pl_util = pd.DataFrame(result[0])
                lp_pool_util1 = pd.DataFrame(result[1])
                lp_pool_util2 = pd.DataFrame(result[2])
                ls_count_open = pd.DataFrame(result[3])
                ls_count_closed = pd.DataFrame(result[4])
                lp_count_open = pd.DataFrame(result[5])
                lp_count_closed = pd.DataFrame(result[6])
                ls_interest = pd.DataFrame(result[7])
                lp_interest = pd.DataFrame(result[8])
                tr_rewards = pd.DataFrame(result[9])
                ls_repayment = pd.DataFrame(result[10])
                lp_repayment = pd.DataFrame(result[11])
                nolus_price = pd.DataFrame(result[12])

                if PL_Utilization.empty:
                    PL_Utilization = pl_util
                    LP_Pool_Util_1 = lp_pool_util1
                    LP_Pool_Util_2 = lp_pool_util2
                    LS_Count_Open = ls_count_open
                    LS_Count_Closed =ls_count_closed
                    LP_Count_Open =lp_count_open
                    LP_Count_Closed = lp_count_closed
                    LS_Interest_mc = ls_interest
                    LP_Interest = lp_interest
                    TR_Rewards = tr_rewards
                    LS_Repayment_mc = ls_repayment
                    LP_Repayment = lp_repayment
                    MC_Nolus_price = nolus_price
                else:
                    PL_Utilization = pd.merge(PL_Utilization,pl_util,on='timestamp', how="left")
                    LP_Pool_Util_1 = pd.merge(LP_Pool_Util_1,lp_pool_util1,on='timestamp', how="left")
                    LP_Pool_Util_2 = pd.merge(LP_Pool_Util_2,lp_pool_util2,on='timestamp', how="left")
                    LS_Count_Open = pd.merge(LS_Count_Open,ls_count_open,on='timestamp', how="left")
                    LS_Count_Closed = pd.merge(LS_Count_Closed,ls_count_closed,on='timestamp', how="left")
                    LP_Count_Open = pd.merge(LP_Count_Open,lp_count_open,on='timestamp', how="left")
                    LP_Count_Closed = pd.merge(LP_Count_Closed,lp_count_closed,on='timestamp', how="left")
                    LS_Interest_mc = pd.merge(LS_Interest_mc,ls_interest,on='timestamp', how="left")
                    LP_Interest = pd.merge(LP_Interest,lp_interest,on='timestamp', how="left")
                    TR_Rewards = pd.merge(TR_Rewards,tr_rewards,on='timestamp', how="left")
                    LS_Repayment_mc = pd.merge(LS_Repayment_mc,ls_repayment,on='timestamp', how="left")
                    LP_Repayment = pd.merge(LP_Repayment,lp_repayment,on='timestamp', how="left")
                    MC_Nolus_price = pd.merge(MC_Nolus_price,nolus_price,on="timestamp", how = "left")
                #todo: Analitics
                # ls_opening = pd.DataFrame(result[0])
                # ls_repayment = pd.DataFrame(result[1])
                # ls_liq = pd.DataFrame(result[2])
                # process_count = process_count+1
                # if LS_Opening_analisys.empty:
                #     LS_Opening_analisys = ls_opening
                #     LS_Repayment_analisys = ls_repayment
                #     LS_Liquidation_analisys = ls_liq
                # else:
                #     LS_Opening_analisys = pd.concat([LS_Opening_analisys, ls_opening], axis=0)
                #     LS_Repayment_analisys = pd.concat([LS_Repayment_analisys, ls_repayment], axis=0)
                #     LS_Liquidation_analisys = pd.concat([LS_Liquidation_analisys, ls_liq], axis=0)
            except:
                print("Process failed!")
                pass
    print("MC_Processes: All processes finished!")
    toc = time.perf_counter()
    print("Processes succesfully finished:"+str(process_count)+"\n Time(s):"+str(toc - tic) +" ")

    print("MC_Output: Processing results:")
    MC_output = pd.DataFrame({"timestamp": PL_Utilization["timestamp"]})
    MC_output["PL_Utilization_mean"] = PL_Utilization.mean(axis=1)
    MC_output["PL_Utilization_min"] = PL_Utilization.min(axis=1)
    MC_output["PL_Utilization_max"] = PL_Utilization.max(axis=1)
    MC_output["LP_Pool_Util_1_mean"] = LP_Pool_Util_1.mean(axis=1)
    MC_output["LP_Pool_Util_1_min"] = LP_Pool_Util_1.min(axis=1)
    MC_output["LP_Pool_Util_1_max"] = LP_Pool_Util_1.max(axis=1)
    MC_output["LP_Pool_Util_2_mean"] = LP_Pool_Util_2.mean(axis=1)
    MC_output["LP_Pool_Util_2_min"] = LP_Pool_Util_2.min(axis=1)
    MC_output["LP_Pool_Util_2_max"] = LP_Pool_Util_2.max(axis=1)
    MC_output["LS_Count_Open_mean"] = LS_Count_Open.mean(axis=1)
    MC_output["LS_Count_Open_min"] = LS_Count_Open.min(axis=1)
    MC_output["LS_Count_Open_max"] = LS_Count_Open.max(axis=1)
    MC_output["LP_Count_Open_mean"] = LP_Count_Open.mean(axis=1)
    MC_output["LP_Count_Open_min"] = LP_Count_Open.min(axis=1)
    MC_output["LP_Count_Open_max"] = LP_Count_Open.max(axis=1)
    MC_output["LS_Count_Closed_mean"] = LS_Count_Closed.mean(axis=1)
    MC_output["LS_Count_Closed_min"] = LS_Count_Closed.min(axis=1)
    MC_output["LS_Count_Closed_max"] = LS_Count_Closed.max(axis=1)
    MC_output["LP_Count_Closed_mean"] = LP_Count_Closed.mean(axis=1)
    MC_output["LP_Count_Closed_min"] = LP_Count_Closed.min(axis=1)
    MC_output["LP_Count_Closed_max"] = LP_Count_Closed.max(axis=1)
    MC_output["LS_Interest_mean"] = LS_Interest_mc.mean(axis=1)
    MC_output["LS_Interest_min"] = LS_Interest_mc.min(axis=1)
    MC_output["LS_Interest_max"] = LS_Interest_mc.max(axis=1)
    MC_output["LP_Interest_mean"] = LP_Interest.mean(axis=1)
    MC_output["LP_Interest_min"] = LP_Interest.min(axis=1)
    MC_output["LP_Interest_max"] = LP_Interest.max(axis=1)
    MC_output["TR_Rewards_mean"] = TR_Rewards.mean(axis=1)
    MC_output["TR_Rewards_min"] = TR_Rewards.min(axis=1)
    MC_output["TR_Rewards_max"] = TR_Rewards.max(axis=1)
    MC_output["LS_Repayment_mean"] = LS_Repayment_mc.mean(axis=1)
    MC_output["LS_Repayment_min"] = LS_Repayment_mc.min(axis=1)
    MC_output["LS_Repayment_max"] = LS_Repayment_mc.max(axis=1)
    MC_output["LP_Repayment_mean"] = LP_Repayment.mean(axis=1)
    MC_output["LP_Repayment_min"] = LP_Repayment.min(axis=1)
    MC_output["LP_Repayment_max"] = LP_Repayment.max(axis=1)
    MC_output["MC_Nolus_price_mean"] = MC_Nolus_price.mean(axis=1)
    MC_output["MC_Nolus_price_min"] = MC_Nolus_price.min(axis=1)
    MC_output["MC_Nolus_price_max"] = MC_Nolus_price.max(axis=1)
    print("Finalization: recording results")
    MC_output.to_csv("PBI_MC/MC_Output.csv", index=False)
    PL_Utilization.to_csv("PBI_MC/PL_Utilization.csv", index=False)
    LP_Pool_Util_1.to_csv("PBI_MC/LP_Pool_Util_1.csv", index=False)
    LP_Pool_Util_2.to_csv("PBI_MC/LP_Pool_Util_2.csv", index=False)
    LS_Count_Open.to_csv("PBI_MC/LS_Count_Open.csv", index=False)
    LP_Count_Open.to_csv("PBI_MC/LP_Count_Open.csv", index=False)
    LS_Count_Closed.to_csv("PBI_MC/LS_Count_Closed.csv", index=False)
    LP_Count_Closed.to_csv("PBI_MC/LP_Count_Closed.csv", index=False)
    LS_Interest_mc.to_csv("PBI_MC/LS_Interest.csv", index=False)
    LP_Interest.to_csv("PBI_MC/LP_Interest.csv", index=False)
    TR_Rewards.to_csv("PBI_MC/TR_Rewards.csv", index=False)
    LS_Repayment_mc.to_csv("PBI_MC/LS_Repayment.csv", index=False)
    LP_Repayment.to_csv("PBI_MC/LP_Repayment.csv", index=False)
    MC_Nolus_price.to_csv("PBI_MC/MC_Nolus_price.csv", index=False)
    print("Finalization: recording results")

