import pandas as pd
import requests
import datetime
import numpy as np
import time


def MP_Assets_Download(args):
    mp_all = pd.DataFrame()
    assets = args["Active_Assets"]
    days = args["N"] + 10
    for ids in assets:
        time.sleep(10)
        df_1 = requests.get('https://api.coingecko.com/api/v3/coins/' + ids + '/market_chart',
                            params={"vs_currency": "usd", "days": "max", "interval": "daily"})
        df_2 = requests.get('https://api.coingecko.com/api/v3/coins/' + ids + '/ohlc',
                            params={"vs_currency": "usd", "days": "max"})
        df_1 = pd.DataFrame.from_dict(df_1.json())
        df_2 = pd.DataFrame.from_dict(df_2.json())
        # prepare p1 of the main dataframe
        df_1[["MP_timestamp", "MP_price_in_stable"]] = df_1["prices"].tolist()
        df_1[["timestampmc", "MP_marketcap"]] = df_1["market_caps"].tolist()
        df_1[["timestamptv", "MP_volume"]] = df_1["total_volumes"].tolist()
        df_1 = df_1[["MP_timestamp", "MP_price_in_stable", "MP_marketcap", "MP_volume"]]
        # prepare p2 of the main dataframe
        df_2[["MP_timestamp", "MP_price_open", "MP_price_high", "MP_price_low", "MP_price_close"]] = df_2
        df_2 = df_2[["MP_timestamp", "MP_price_open", "MP_price_high", "MP_price_low", "MP_price_close"]]
        # merge dataframes`
        df_all = pd.merge(df_1, df_2, how="left", on="MP_timestamp")
        # add symbol column
        df_all["MP_asset_symbol"] = ids
        # linear interpolation
        df_all = df_all.interpolate(method="linear")
        # chanege unix date to real date
        df_all["MP_timestamp"] = df_all["MP_timestamp"].apply(lambda x: x / 1000)
        df_all["MP_timestamp"] = df_all["MP_timestamp"].apply(datetime.date.fromtimestamp)
        # cut last x days
        df_all = df_all[-days:-1]
        mp_all = mp_all.append(df_all, ignore_index="True", verify_integrity="True")
    MP_Asset_State = mp_all[
        ["MP_asset_symbol", "MP_timestamp", "MP_price_open", "MP_price_high", "MP_price_low", "MP_price_close",
         "MP_volume", "MP_marketcap"]]
    MP_Asset = mp_all[["MP_asset_symbol", "MP_timestamp", "MP_price_in_stable"]]
    MP_Asset.to_csv("MP_Asset_raw.csv")
    MP_Asset_State.to_csv("MP_Asset_State_raw.csv")

    # transform MP_Asset from usd to currency_stable
    return MP_Asset, MP_Asset_State


def MP_Assets_Daily(args):
    currency_stable = args["currency_stable"]
    try:
        with open("MP_Asset_State_raw.csv", "r") as outfile:
            MP_Asset_State = pd.read_csv(outfile, index_col=0)
        with open("MP_Asset_raw.csv", "r") as outfile:
            MP_Asset = pd.read_csv(outfile, index_col=0)
    except:
        MP_Asset, MP_Asset_State = MP_Assets_Download(args)

    max_timestamp = MP_Asset.drop_duplicates(subset="MP_asset_symbol", keep="last").min()
    #print(max_timestamp)
    MP_Asset = MP_Asset.loc[MP_Asset["MP_timestamp"] < max_timestamp["MP_timestamp"]]
    #print(MP_Asset.drop_duplicates(subset="MP_asset_symbol", keep="last"))
    MP_Asset_State = MP_Asset_State.drop(
        MP_Asset_State[MP_Asset_State["MP_timestamp"] >= max_timestamp["MP_timestamp"]].index)

    timestamps = MP_Asset.drop_duplicates(subset="MP_timestamp", keep="last")
    timestamps = timestamps["MP_timestamp"].values
    timestamps = timestamps[-args["N"]:]
    MP_Asset_new = pd.DataFrame()
    for asset in args["Active_Assets"]:
        #todo: to be made mp_asset_state values
        vals = pd.DataFrame({"MP_asset_symbol":np.repeat(asset,len(timestamps)),"MP_timestamp":timestamps})
        vals = pd.merge(vals,MP_Asset, on=["MP_asset_symbol","MP_timestamp"],how='left')
        vals["MP_price_in_stable"] = vals["MP_price_in_stable"].fillna(method="ffill")
        vals = vals.dropna()
        MP_Asset_new = pd.concat([MP_Asset_new,vals],axis=0,ignore_index=True)
    MP_Asset = MP_Asset_new
    #MP_Asset = MP_Asset.loc[MP_Asset["MP_timestamp"].isin(timestamps)]
    MP_Asset_State = MP_Asset_State.loc[MP_Asset_State["MP_timestamp"].isin(timestamps)]
    # transform MP_Asset from usd to currency_stable
    cp = MP_Asset[MP_Asset.loc[:, "MP_asset_symbol"] == currency_stable]
    cpis = cp.drop(["MP_asset_symbol"], axis=1)
    cpis.rename(columns={"MP_price_in_stable": "price_in_stable_date"}, inplace=True)

    MP_Asset = MP_Asset[MP_Asset.loc[:, "MP_asset_symbol"] != currency_stable]

    MP_Asset = pd.merge(MP_Asset, cpis, on="MP_timestamp", how="left")
    MP_Asset["MP_price_in_stable"] = MP_Asset["MP_price_in_stable"] * MP_Asset["price_in_stable_date"]
    MP_Asset = MP_Asset.drop(["price_in_stable_date"], axis=1)
    MP_Asset = pd.concat([MP_Asset, cp], ignore_index=True)
    not_transforming = MP_Asset_State[MP_Asset_State.loc[:, "MP_asset_symbol"] == currency_stable]
    MP_Asset_State = MP_Asset_State[MP_Asset_State.loc[:, "MP_asset_symbol"] != currency_stable]

    MP_Asset_State = pd.merge(MP_Asset_State, cpis, on="MP_timestamp", how='left')

    MP_Asset_State[["MP_price_open", "MP_price_high", "MP_price_low", "MP_price_close",
                    "MP_volume", "MP_marketcap"]].multiply(MP_Asset_State["price_in_stable_date"], axis="index")

    MP_Asset_State = MP_Asset_State.drop(["price_in_stable_date"], axis=1)

    MP_Asset_State = pd.concat([MP_Asset_State, not_transforming], ignore_index=True)

    # convert all prices to int
    # ls = pd.DataFrame(args["symbol_digit"])
    # ls = ls.rename(columns={"symbol": "MP_asset_symbol"})
    # MP_Asset["digit"] = ls.loc[ls["MP_asset_symbol"] == args["currency_stable"]]["digit"].values[
    #   0]  # pd.merge(MP_Asset, ls, on='MP_asset_symbol', how="left")
    # MP_Asset_State["digit"] = ls.loc[ls["MP_asset_symbol"] == args["currency_stable"]]["digit"].values[
    #    0]  # pd.merge(MP_Asset_State, ls, on='MP_asset_symbol', how="left")
    #
    # MP_Asset_State[["MP_price_open", "MP_price_high", "MP_price_low", "MP_price_close"]] = MP_Asset_State[
    #    ["MP_price_open", "MP_price_high", "MP_price_low", "MP_price_close"]].multiply(10 ** MP_Asset_State["digit"],
    #                                                                                  axis="index")  # .round(0).astype(int)
    # MP_Asset_State = MP_Asset_State.drop(["digit"], axis=1)
    # MP_Asset[["MP_price_in_stable"]] = MP_Asset[["MP_price_in_stable"]].multiply(10 ** MP_Asset["digit"],
    #                                                                             axis="index").round(0).astype("uint64")
    # MP_Asset = MP_Asset.drop(["digit"], axis=1)
    #
    # MP_Asset_State[["MP_volume", "MP_marketcap"]] = MP_Asset_State[["MP_volume", "MP_marketcap"]].astype("uint64")
    MP_Asset.to_csv("MP_ASSET.csv")
    MP_Asset_State.to_csv("MP_ASSET_STATE.csv")
    return MP_Asset, MP_Asset_State