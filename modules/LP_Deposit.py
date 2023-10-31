import pandas as pd
import numpy as np
import modules.gnrl as gnrl


def LP_Deposit_Generate(MP_Asset, pool_id, args):
    names = ["LP_asset_symbol", "LP_timestamp", "LP_address_id", "LP_deposit_height"]
    id_prefix = ["lpaid", "lpcid"]
    LP_Deposit = gnrl.timestamps_generation(MP_Asset, names, id_prefix, args["new_LP_opened_daily_count"], args)
    lp_dep_h = [s for s in range(100, 100 + len(LP_Deposit))]
    lp_dep_idx = lp_dep_h
    LP_Deposit = LP_Deposit.drop(["LP_asset_symbol"], axis=1)

    LP_Deposit["LP_deposit_height"] = lp_dep_h
    LP_Deposit["LP_deposit_idx"] = lp_dep_idx
    np.random.seed(args["seed"])
    args["seed"] = args["seed"] + 1
    lp_list = np.random.choice(args["Pool_Assets"], len(LP_Deposit))
    LP_Deposit["LP_symbol"] = lp_list
    pool_id.rename(columns={"LS_loan_pool_id": "LP_pool_id"})
    LP_Deposit = pd.merge(LP_Deposit, pool_id, on="LP_symbol", how="left")
    LP_Deposit["LP_amnt_asset"] = gnrl.f_dist(args["LP_amnt_stable_df_num"],
                                              args["LP_amnt_stable_df_den"],
                                              args["LP_amnt_stable_min"],
                                              args["LP_amnt_stable_max"],
                                              len(LP_Deposit), args, args["LP_multiplyer"])
    LP_Deposit["LP_amnt_asset"] = LP_Deposit["LP_amnt_asset"].multiply(args["contract_weight"])
    LP_Deposit["LP_amnt_asset"] = LP_Deposit["LP_amnt_asset"].round(0).astype("uint64")
    symbol_digit = pd.DataFrame(args["symbol_digit"])
    symbol_digit = symbol_digit.rename(columns={"symbol": "LP_symbol"})
    LP_Deposit = pd.merge(LP_Deposit, symbol_digit, on="LP_symbol", how='left')
    MP_Asset = MP_Asset.rename(columns={"MP_timestamp": "LP_timestamp", "MP_asset_symbol": "LP_symbol"})
    LP_Deposit = pd.merge(LP_Deposit, MP_Asset, on=["LP_timestamp", "LP_symbol"], how='left')

    #LP_Deposit["LP_amnt_asset"] = LP_Deposit["LP_amnt_asset"].multiply(10 ** LP_Deposit["digit"]).astype("uint64")
    #LP_Deposit["MP_price_in_stable"] = LP_Deposit["MP_price_in_stable"].div(10 ** LP_Deposit["digit"])
    LP_Deposit.loc[(LP_Deposit["LP_symbol"] == args["currency_stable"]), 'MP_price_in_stable'] = 1

    LP_Deposit["LP_amnt_stable"] = LP_Deposit["LP_amnt_asset"] * LP_Deposit["MP_price_in_stable"]

    LP_Deposit["LP_amnt_stable"] = LP_Deposit["LP_amnt_stable"].round(0).astype("uint64")
    LP_Deposit = LP_Deposit.drop(["digit", "MP_price_in_stable", "LP_symbol"], axis=1)

    LP_Deposit["SYS_LP_expected_duration"] = gnrl.f_dist(args["LS_cltr_amnt_asset_df_num"],
                                                         args["LS_cltr_amnt_asset_df_den"],
                                                         args["SYS_LS_expected_payment_min"],
                                                         args["SYS_LS_expected_payment_max"], len(LP_Deposit),
                                                         args, args["SYS_LS_expected_payment_extremum"])
    LP_Deposit["SYS_LP_expected_duration"] = LP_Deposit["SYS_LP_expected_duration"] - 13
    LP_Deposit["SYS_LP_expected_duration"] = LP_Deposit["SYS_LP_expected_duration"] * -1
    LP_Deposit["SYS_LP_expected_duration"] = LP_Deposit["SYS_LP_expected_duration"].round(0).astype(int)
    LP_Deposit["SYS_LP_contract_id"] = LP_Deposit["LP_address_id"] + LP_Deposit["LP_deposit_idx"].astype(str)
    # LP_Depo
    return LP_Deposit

