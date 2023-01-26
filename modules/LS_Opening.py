import pandas as pd
import numpy as np
import modules.gnrl as gnrl

def ls_lsymbol(digit, symbol):
    ll = []
    for i in range(0, len(digit), 1):
        ll.append(tuple([symbol[i][0], None, digit[i]]))
    ls = pd.DataFrame({"LS_loan_symbol": ll})
    return ls

def int_price_digit(str_vals):  # "3211.234565e-05"
    digit = []
    intprice = []
    for i in str_vals:
        a, b = i[0].split('.')
        try:
            intprice.append(int(a + b))
            digit.append(len(b))
        except:
            a, c = i[0].split('e')
            a, b = a.split('.')
            x = len(b) - int(c)
            intprice.append(int(a + b))
            digit.append(x)
    return intprice, digit

def LS_loan_symbol(MP_Asset,args):
    uniques = MP_Asset.drop_duplicates(subset=["MP_asset_symbol"], keep="last")
    uniques[["MP_price_in_stable"]] = uniques[["MP_price_in_stable"]].astype(str)
    #str_vals = uniques[["MP_price_in_stable"]].values.tolist()
    symbol_vals = uniques[["MP_asset_symbol"]].values.tolist()
    #intprice, digit = int_price_digit(str_vals)
    intprice = pd.DataFrame(args["symbol_digit"])
    digit = intprice["digit"].values.tolist()
    ls = ls_lsymbol(digit, symbol_vals)
    LS_Loan_symbol = pd.DataFrame()
    LS_Loan_symbol[["LS_asset_symbol"]] = symbol_vals
    LS_Loan_symbol[["LS_loan_symbol"]] = ls
    tdf = pd.DataFrame({"symbol":symbol_vals,"digit":digit})
    return LS_Loan_symbol


def LS_loan_amnt_stable_asset(MP_Asset, LS_Openings, args):
    df_num = args["LS_cltr_amnt_asset_df_num"]
    df_den = args["LS_cltr_amnt_asset_df_den"]
    #df_min = args["LS_loan_amnt_asset_min"]
    #df_max = args["LS_loan_amnt_asset_max"]
    df_min_cltr = args["LS_cltr_amnt_asset_min"]  # change names max asset
    df_max_cltr = args["LS_cltr_amnt_asset_max"]

    # MP_Asset = MP_Asset
    # str_vals = MP_Asset[["MP_price_in_stable"]].astype(str).values.tolist()
    # intprice, digit = int_price_digit(str_vals)
    sym_digit = pd.DataFrame(args["symbol_digit"])
    sym_digit.rename(columns={"symbol": "LS_asset_symbol"}, inplace=True)
    digit = sym_digit["digit"].values.tolist()

    mp = pd.DataFrame(
        {"LS_asset_symbol": MP_Asset["MP_asset_symbol"], "LS_timestamp": MP_Asset["MP_timestamp"],
         "MP_price_in_stable": MP_Asset["MP_price_in_stable"]})
    ls = pd.DataFrame({"LS_asset_symbol": LS_Openings["LS_asset_symbol"], "LS_timestamp": LS_Openings["LS_timestamp"]})#   "LS_loan_amnt_asset": gnrl.f_dist(df_num, df_den, df_min, df_max, len(LS_Openings), args,multiply=args["LS_amnt_multiplyer"])})
    ls = pd.merge(ls, mp, on=["LS_asset_symbol", "LS_timestamp"], how='left')
    #check for non-existing values
    samples_with_no_price = ls.loc[ls["MP_price_in_stable"].isna(),["LS_asset_symbol"]].drop_duplicates()["LS_asset_symbol"]
    #samples_with_price = [value for value in args["Active_Assets"] if value not in samples_with_no_price]
    while not samples_with_no_price.empty:
        temp_df = pd.DataFrame({"asset":args["Active_Assets"],"distribution":args["Active_Assets_Distribution"]})
        temp_df = temp_df.drop(temp_df.loc[temp_df["asset"].isin(samples_with_no_price)].index,axis=0)
        Asset = temp_df["asset"].values
        distribution = temp_df["distribution"].values
        samples = np.random.choice(Asset, size=len(ls.loc[ls["MP_price_in_stable"].isna(),["LS_asset_symbol"]]),
                                   p=distribution/sum(distribution))
        #replace missing asset with existing one
        ls.loc[ls["MP_price_in_stable"].isna(), ["LS_asset_symbol"]] = samples
        #reassign mp_price_in_stable
        ls = ls.drop(columns=["MP_price_in_stable"],axis=0)
        ls = pd.merge(ls, mp, on=["LS_asset_symbol", "LS_timestamp"], how='left')
        samples_with_no_price = ls.loc[ls["MP_price_in_stable"].isna(), ["LS_asset_symbol"]].drop_duplicates()[
            "LS_asset_symbol"]
    ls = pd.merge(ls, sym_digit, on=["LS_asset_symbol"], how='left')

    #todo: check for na and replace with new values

    #ls["MP_loan_amnt_stable"] = ls["MP_price_in_stable"].astype("uint64")
    num=args["LS_loan_amnt_asset_stable_df_num"]
    den=args["LS_loan_amnt_asset_stable_df_den"]
    minval=args["LS_loan_amnt_asset_stable_min"]
    maxval=args["LS_loan_amnt_asset_stable_max"]
    multiplyer=args["LS_loan_amnt_asset_stable_multiplyer"]
    length = len(ls)
    ls["LS_loan_amnt_stable"]=gnrl.f_dist(num,den,minval,maxval,length,args,multiply=multiplyer)
        #ls.loc[ls["LS_asset_symbol"]==asset,"LS_loan_amnt_asset"]=gnrl.f_dist(num,den,0.01,1.5,length,args,10000)
        #ls = pd.merge(ls,temp_df,on=["LS_asset_symbol","LS_timestamp"],how='left')
    #digit = pd.DataFrame(ls["LS_loan_symbol"].tolist(), index=ls.index)
    # loans
    ls["LS_loan_amnt_stable"] = ls["LS_loan_amnt_stable"]#.round(0).astype("uint64")
    ls["LS_loan_amnt_stable"] = ls["LS_loan_amnt_stable"]#.multiply(10**sym_digit.loc[sym_digit["LS_asset_symbol"]==args["currency_stable"],"digit"].values[0])

    ls["LS_loan_amnt_asset"] = ls["LS_loan_amnt_stable"]/ls["MP_price_in_stable"]
    # colaterals
    ls["LS_cltr_symbol"] = ls["LS_asset_symbol"]
    ls["LS_cltr_amnt_asset"] = gnrl.f_dist(df_num, df_den, df_min_cltr, df_max_cltr, len(LS_Openings), args) * ls[
        "LS_loan_amnt_asset"]
    ls["LS_cltr_amnt_stable"] = ls["LS_cltr_amnt_asset"] * ls["MP_price_in_stable"]
    ls["LS_cltr_amnt_stable"] = ls["LS_cltr_amnt_stable"]#.round(0).astype("uint64")
    ls["SYS_LS_cltr_amnt_asset"] = ls["LS_cltr_amnt_asset"]
    ls["SYS_LS_loan_amnt_asset"] = ls["LS_loan_amnt_asset"]
    #ls["LS_cltr_amnt_asset"] = ls["LS_cltr_amnt_asset"].multiply(10 ** ls["digit"]).round(0).astype("uint64")
    #ls["LS_loan_amnt_asset"] = ls["LS_loan_amnt_asset"].multiply(10 ** ls["digit"]).round(0).astype("uint64")
   # ls = ls.drop(["digit"], axis=1)
    return ls


def LS_Opening_Generate(MP_Asset, pool_id, args,name=""):


    # generate timestamps and ids
    names = ["LS_asset_symbol", "LS_timestamp", "LS_contract_id", "LS_address_id"]
    id_prefix = [""+name+"LScid", ""+name+"LS_aid"]
    LS_Opening = gnrl.timestamps_generation(MP_Asset, names, id_prefix, args["new_LS_opened_daily_count"], args)
    #check
    # loan amnt stable and loan amnt asset cltr
    ll_as = LS_loan_amnt_stable_asset(MP_Asset, LS_Opening, args)
    LS_Opening["LS_loan_amnt_asset"] = ll_as[["LS_loan_amnt_asset"]]#.astype("uint64")
    LS_Opening["LS_loan_amnt_stable"] = ll_as["LS_loan_amnt_stable"]#.astype("uint64")
    LS_Opening["LS_asset_symbol"] = ll_as["LS_asset_symbol"]
    ll_sym = LS_loan_symbol(MP_Asset, args)
    LS_Opening = pd.merge(LS_Opening, ll_sym, how="left", on="LS_asset_symbol")
    # ls_symbol ls_loan
    LS_Opening["LS_cltr_symbol"] = ll_as["LS_cltr_symbol"]
    LS_Opening["LS_cltr_amnt_stable"] = ll_as["LS_cltr_amnt_stable"]#.astype("uint64")
    LS_Opening["LS_cltr_amnt_asset"] = ll_as["LS_cltr_amnt_asset"]#.astype("uint64")
    LS_Opening["SYS_LS_cltr_amnt_asset"] = ll_as["SYS_LS_cltr_amnt_asset"]
    LS_Opening["SYS_LS_loan_amnt_asset"] = ll_as["SYS_LS_loan_amnt_asset"]
    np.random.seed(args["seed"])
    args["seed"] = args["seed"] + 1
    #
    lp_list = np.random.choice(args["Pool_Assets"], len(LS_Opening))
    LS_Opening["LP_symbol"] = lp_list
    # Price_transformation
    pool_prices = MP_Asset[MP_Asset.loc[:, "MP_asset_symbol"] != args["currency_stable"]]
    pool_prices = pool_prices[pool_prices.loc[:, "MP_asset_symbol"].isin(args["Pool_Assets"])]
    pool_prices = pool_prices.rename(columns={"MP_asset_symbol": "LP_symbol", "MP_timestamp": "LS_timestamp"})
    LS_Opening = pd.merge(LS_Opening, pool_prices, on=["LP_symbol", "LS_timestamp"], how="left")
    symbol_digit = pd.DataFrame(args["symbol_digit"])

    #    MP_Asset = MP_Asset[MP_Asset.loc[:,"MP_asset_symbol"] != currency_stable]
    pool_pd = pd.DataFrame({"LP_symbol": symbol_digit["symbol"], "digit": symbol_digit["digit"]})
    LS_Opening = pd.merge(LS_Opening, pool_pd, on=["LP_symbol"], how="left")
    LS_Opening["MP_price_in_stable"] = LS_Opening["MP_price_in_stable"].fillna(1)
    LS_Opening["MP_price_in_stable"] = LS_Opening["MP_price_in_stable"] / 10 ** LS_Opening["digit"]
    LS_Opening["MP_price_in_stable"].loc[LS_Opening["LP_symbol"] == args["currency_stable"]] = 1
    # LS_Opening["LS_loan_amnt_stable"] = LS_Opening["LS_loan_amnt_stable"].multiply(LS_Opening["MP_price_in_stable"],
    #                                                                                axis="index")#.astype("uint64")
    # LS_Opening["LS_cltr_amnt_stable"] = LS_Opening["LS_cltr_amnt_stable"].multiply(LS_Opening["MP_price_in_stable"],
    #                                                                                axis="index")#.astype("uint64")

    # LS_native_amnt_stable_nolus
    LS_Opening["LS_native_amnt_nolus"] = 0
    LS_Opening["LS_native_amnt_stable"] = 0

    # SYS_LS_expected_duration/penalty
    LS_Opening["SYS_LS_expected_payment"] = gnrl.f_dist(args["LS_cltr_amnt_asset_df_num"],
                                                   args["LS_cltr_amnt_asset_df_den"],
                                                   args["SYS_LS_expected_payment_min"],
                                                   args["SYS_LS_expected_payment_max"], len(LS_Opening), args,
                                                   args["SYS_LS_expected_payment_extremum"])
    LS_Opening["SYS_LS_expected_payment"] = LS_Opening["SYS_LS_expected_payment"].round(0).astype("uint64")
    LS_Opening["SYS_LS_expected_penalty"] = gnrl.f_dist(args["SYS_LS_expected_penalty_df_num"],
                                                   args["SYS_LS_expected_penalty_df_den"],
                                                   args["SYS_LS_expected_penalty_min"],
                                                   args["SYS_LS_expected_penalty_max"], len(LS_Opening), args)
    LS_Opening["SYS_LS_expected_penalty"] = LS_Opening["SYS_LS_expected_penalty"].round(0).astype("uint64")
    # pool_id
    pool_id.rename(columns={"LS_loan_pool_id": "LS_pool_id"})
    LS_Opening = pd.merge(LS_Opening, pool_id, on="LP_symbol", how="left")

    LS_Opening["LS_interest"] = np.nan

    LS_Opening = LS_Opening.drop(["MP_price_in_stable", "digit", "LP_symbol"], axis=1)

    return LS_Opening