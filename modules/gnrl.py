import pandas as pd
import numpy as np
import time
import uuid as uid
import json


#############################################################################################
#gnrl addition
#def add_hyperparameter(Name,Data):
#    with open("config.json", 'r') as f:
#        args = json.load(f)
#    args[""+Name+""] = Data
#    with open("config.json", 'w') as f:
#        json.dump(args,f)
#    return

def f_dist(df_num, df_den, df_min, df_max, length, args, multiply=1):
    np.random.seed(args["seed"])
    args["seed"] = args["seed"] + 1

    if args["method"] == 'large_sample':
        np_arr = np.random.f(df_num, df_den, size=length * 3)
        np_arr = np.delete(np_arr, np.argwhere(np_arr < df_min))
        np_arr = np.delete(np_arr, np.argwhere(np_arr > df_max))
        while len(np_arr) < length:
            np.random.seed(args["seed"])
            args["seed"] = args["seed"] + 1
            np_arr_2 = np.random.f(df_num, df_den, size=length * 3)
            np_arr_2 = np.delete(np_arr_2, np.argwhere(np_arr < df_min))
            np_arr_2 = np.delete(np_arr_2, np.argwhere(np_arr > df_max))

            np_arr = np.concatenate((np_arr, np_arr_2), axis=None)
        if len(np_arr) > length:
            np_arr = np_arr[:length]
    if args["method"] == 'range_normalization':
        np_arr = np.random.f(df_num, df_den, size=length)*multiply
        min_x = min(np_arr)
        max_x = max(np_arr)
        bool_arr = np_arr[(np_arr < df_min) | (np_arr > df_max)]
        np_arr = np_arr[(np_arr >= df_min) & (np_arr <= df_max)]
        bool_arr = ((bool_arr - min_x) / (max_x - min_x)) * (df_max - df_min) + df_min
        np_arr = np.concatenate((np_arr, bool_arr), axis=None)
    return np_arr

def generate_uid(length):
    id1 = []
    id2 = []
    for i in range(length):
        id1.append(str(uid.uuid1()).replace("-",""))
        id2.append(str(uid.uuid1()).replace("-",""))
    return id1, id2

def timestamps_generation(MP_Asset, name_columns, id_prefix, distribution, args):
    #todo - if its not working pass MP_ASSET_STABLE instead
    timestamps = MP_Asset.drop_duplicates(subset=["MP_timestamp"])
    timestamps = timestamps["MP_timestamp"]
    dataframe = pd.DataFrame()
    cid = pd.DataFrame()
    aid = pd.DataFrame()
    # if args["id_generator"] == "uuid":
    #     np.random.seed(args["seed"])
    #     samples = np.random.choice(args["Active_Assets"], size=args["new_LS_opened_daily_count"],
    #                                p=args["Active_Assets_Distribution"])
    #     args["seed"] = args["seed"] + 1
    #     for day in timestamps:
    #         df_samples = pd.DataFrame({"LS_asset_symbol": samples})
    #         df_samples[["LS_timestamp"]] = day
    #         contract_id, address_id = generate_uid(len(df_samples))
    #         cid = cid.append(contract_id, ignore_index="True", verify_integrity="True")
    #         aid = aid.append(address_id, ignore_index="True", verify_integrity="True")
    #         LS_Opening = LS_Opening.append(df_samples, ignore_index="True", verify_integrity="True")
    if args["id_generator"] == "series":
        dataframe, cid, aid = series_tstp_ids_samples(dataframe, timestamps, id_prefix, distribution, args["new_LS_opened_type"],
                                                       args)
    dataframe[["id1"]] = cid
    dataframe[["id2"]] = aid
    dataframe.rename(columns={"symbol":name_columns[0], "timestamp": name_columns[1],
                               "id1":name_columns[2], "id2": name_columns[3]}, inplace=True)
    return dataframe


def series_tstp_ids_samples(dataframe, timestamps, input_str, distribution, mode, args):
    cid = pd.DataFrame()
    aid = pd.DataFrame()

    if mode == "const":
        np.random.seed(args["seed"])
        samples = np.random.choice(args["Active_Assets"], size=distribution * args["N"],
                                   p=args["Active_Assets_Distribution"])
        args["seed"] = args["seed"] + 1
        timestamps = pd.DataFrame(np.repeat(timestamps.values, distribution, axis=0))
        df_samples = pd.DataFrame({"symbol": samples})
        df_samples[["timestamp"]] = timestamps
        dataframe = dataframe.append(df_samples, ignore_index="True", verify_integrity="True")
        contract_id = [input_str[0] + str(sub) for sub in
                       range(1000000, 1000000 + distribution * args["N"])]
        address_id = [input_str[1] + str(sub) for sub in
                      range(1000000, 1000000 + distribution * args["N"])]
        cid = cid.append(contract_id, ignore_index="True", verify_integrity="True")
        aid = aid.append(address_id, ignore_index="True", verify_integrity="True")
        return dataframe, cid, aid
    if mode == "function":
        size_args = pd.read_csv(distribution)
        size_args = size_args["Count"].tolist()
        timestamps = pd.DataFrame(np.repeat(timestamps.values, size_args, axis=0))
        np.random.seed(args["seed"])
        samples = np.random.choice(args["Active_Assets"], size=len(timestamps), p=args["Active_Assets_Distribution"])
        args["seed"] = args["seed"] + 1
        df_samples = pd.DataFrame({"symbol": samples})
        df_samples[["timestamp"]] = timestamps
        dataframe = dataframe.append(df_samples, ignore_index="True", verify_integrity="True")
        contract_id = [input_str[0] + str(sub) for sub in range(1000000, 1000000 + len(timestamps))]
        address_id = [input_str[1] + str(sub) for sub in range(1000000, 1000000 + len(timestamps))]
        cid = cid.append(contract_id, ignore_index="True", verify_integrity="True")
        aid = aid.append(address_id, ignore_index="True", verify_integrity="True")
        return dataframe, cid, aid

def f_dist(df_num, df_den, min_val, max_val, length, args, multiply=1):
    np.random.seed(args["seed"])
    args["seed"] = args["seed"] + 1

    if args["method"] == 'large_sample':
        np_arr = np.random.f(df_num, df_den, size=length)*multiply
        np_arr = np.delete(np_arr, np.argwhere(np_arr < min_val))
        np_arr = np.delete(np_arr, np.argwhere(np_arr > max_val))
        while len(np_arr) < length:
            np.random.seed(args["seed"])
            args["seed"] = args["seed"] + 1
            np_arr_2 = np.random.f(df_num, df_den, size=length)*multiply
            np_arr_2 = np.delete(np_arr_2, np.argwhere(np_arr_2 < min_val))
            np_arr_2 = np.delete(np_arr_2, np.argwhere(np_arr_2 > max_val))

            np_arr = np.concatenate((np_arr, np_arr_2), axis=None)
        if len(np_arr) > length:
            np_arr = np_arr[:length]
    if args["method"] == 'range_normalization':
        np_arr = np.random.f(df_num, df_den, size=length)*multiply
        min_x = min(np_arr)
        max_x = max(np_arr)
        bool_arr = np_arr[(np_arr < min_val) | (np_arr > max_val)]
        np_arr = np_arr[(np_arr >= min_val) & (np_arr <= max_val)]
        bool_arr = ((bool_arr - min_x) / (max_x - min_x)) * (max_val - min_val) + min_val
        np_arr = np.concatenate((np_arr, bool_arr), axis=None)
        np_arr
    return np_arr

###################################################################################
def md(x, y, dsp=True, prc=False):  # max difference - default is absolute, and when prc=True - relative difference in [%]
    if isinstance(x, (int, float)): x = c_([x])
    if isinstance(y, (int, float)): y = c_([y])
    if np.issubdtype(x.dtype, str):
        res = np.array_equal(x.astype('U'), y.astype('U'))
    else:
        if x.ndim == y.ndim and np.all(x.shape == y.shape):
            res = np.max((np.max(np.abs(x - y))))
            if prc:
                x1 = nans(size=x.shape)
                x1=x+0
                x1[x==0] = 1e-6
                res2 = np.max((np.max(np.abs((x - y)/x1))))
            dres = 'max_abs_dif: ' + str(res)
            if prc: dres = dres + '\n max_rel_dif: ' + str(res2) + '%'
        else:
            res = -1
            dres = 'x.shape: ' + str(x.shape) + '   AND   y.ndim: ' + str(y.shape)
        if dsp == True: print(dres)
    return res

##############################################################################
def nans(size, size2=None):
    if size2 is None:
        if isinstance(size, (int, np.int64, float)): size = [size, size]
        if len(size) == 1:
            size = (int(size[0]),)
        else:
            size = int(size[0]), int(size[1])
    else:
        try:
            if isinstance(size, (int, np.int64, float)):
                size = int(size), int(size2)
            else:
                size = int(size[0]), int(size[1]), int(size2)  # for 3d tensors
        except:
            raise Exception('In NANS(): Incorrect input parameters. Should be nans((<rows>, <cols>)), nans(<rows>, <cols>) or nans(<rows_cols>).')
    res = np.full(size, np.nan)
    return res

##############################################################################
def timestamp(start_date, N):
    res = pd.date_range(start=start_date, periods=N)
    return res

##############################################################################
def c_(x):
    if isinstance(x, (int, float, str, np.int64)): x = np.array([x])
    return np.reshape(x, (len(x), 1))

##############################################################################
def rand(size=1, l=0, h=1, tp='float', seed=None):
    if isinstance(size, (int, float)): size = [size, size]
    np.random.seed(seed=seed)
    if tp=='float': r = np.random.uniform(low=l, high=h, size=size)
    if tp=='int':   r = np.random.randint(low=l, high=h+1, size=size)
    return r

##############################################################################
def randn(size=1, m=0, s=1, seed=None):
    if isinstance(size, (int, float)): size = [size, size]
    np.random.seed(seed=seed)
    r = np.random.normal(loc=m, scale=s, size=size)
    return r

##############################################################################
def randp(size=1, lam=1, tp='float', seed=None):
    if isinstance(size, (int, float)): size = [size, size]
    np.random.seed(seed=seed)
    r = np.random.poisson(lam=lam, size=size)
    return r

##############################################################################
def init_dfs(N_days, N_LS, N_LP, N_pools, N_asset):
    # MP_Asset
    cols = ['MP_asset_symbol',
            'MP_asset_timestamp',
            'MP_price_in_stable']
    m = len(cols)
    N = N_days*N_asset
    MP_Asset = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)

    # MP_Asset_Daily
    cols = ['MP_asset_symbol',
            'MP_date',
            'MP_price_open',
            'MP_price_high',
            'MP_price_low',
            'MP_price_close',
            'MP_volume',
            'MP_marketcap']
    m = len(cols)
    N = N_days*N_asset
    MP_Asset_Daily = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)

    # LS_Opening
    cols = ['LS_contract_id',
            'LS_address_id',
            'LS_asset_symbol',
            'LS_interest',
            'LS_timestamp',
            'LS_loan_symbol',
            'LS_loan_amnt_stable',
            'LS_loan_amnt_asset',
            'LS_cltr_symbol',
            'LS_cltr_amnt_stable',
            'LS_cltr_amnt_asset',
            'LS_native_amnt_stable',
            'LS_native_amnt_nolus',
            'SYS_LS_expected_duration',
            'SYS_LS_expected_penalty_count',
            'SYS_LS_expected_total_due_amnt']
    m = len(cols)
    N = N_LS
    LS_Opening = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)

    # LS_Closing
    cols = ['LS_contract_id',
            'LS_timestamp']
    m = len(cols)
    N = N_LS # real size is smaller
    LS_Closing = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)

    # LS_Repayment
    cols = ['LS_repayment_id',
            'LS_contract_id',
            'LS_symbol',
            'LS_amnt_stable',
            'LS_timestamp',
            'LS_loan_close',
            'LS_prev_margin_stable',
            'LS_prev_interest_stable',
            'LS_current_margin_stable',
            'LS_current_interest_stable',
            'LS_principle_stable']
    m = len(cols)
    N = N_days
    LS_Repayment = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)

    # LS_Liquidation
    cols = ['LS_liquidation_id',
            'LS_contract_id',
            'LS_symbol',
            'LS_timestamp',
            'LS_amnt_stable',
            'LS_transaction_type',
            'LS_prev_margin_stable',
            'LS_prev_interest_stable',
            'LS_current_margin_stable',
            'LS_current_interest_stable',
            'LS_principle_stable']
    m = len(cols)
    N = N_LS # real size is smaller
    LS_Liquidation = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)

    # LS_CurrentState_Daily
    cols = ['LS_contract_id',
            'LS_timestamp',
            'LS_prev_margin_stable',
            'LS_prev_interest_stable',
            'LS_current_margin_stable',
            'LS_current_interest_stable',
            'LS_principle_stable',
            'LS_amount_stable']
    m = len(cols)
    N = N_days
    LS_CurrentState_Daily = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)

    # LP_Deposit
    cols = ['LP_deposit_id',
            'LP_address_id',
            'LP_timestamp',
            'LP_Pool_id',
            'LP_amnt_stable',
            'LP_amnt_asset',
            'LP_amnt_receipts',
            'SYS_LP_expected_duration']
    m = len(cols)
    N = N_LP
    LP_Deposit = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)

    # LP_Withdraw
    cols = ['LP_withdraw_id',
            'LP_address_id',
            'LP_timestamp',
            'LP_Pool_id',
            'LP_amnt_stable',
            'LP_amnt_asset',
            'LP_amnt_receipts',
            'LP_deposit_close']
    m = len(cols)
    N = N_LS # real size is smaller
    LP_Withdraw = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)

    # LP_Lender_CurrentState
    cols = ['LP_Lender_id',
            'LP_Pool_id',
            'LP_timestamp',
            'LP_Lender_stable',
            'LP_Lender_asset',
            'LP_Lender_receipts']
    m = len(cols)
    N = N_days
    LP_Lender_CurrentState = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)

    # LP_Pool
    cols = ['LP_Pool_id',
            'LP_symbol']
    m = len(cols)
    N = N_pools
    LP_Pool = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)

    # LP_Pool_CurrentState
    cols = ['LP_Pool_id',
            'LP_Pool_timestamp',
            'LP_Pool_total_deposited_stable',
            'LP_Pool_total_deposited_asset',
            'LP_Pool_total_issued_receipts',
            'LP_Pool_total_borrowed_stable',
            'LP_Pool_total_borrowed_asset',
            'LP_Pool_total_yield_stable',
            'LP_Pool_total_yield_asset']
    m = len(cols)
    N = N_days
    LP_Pool_CurrentState = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)

    # MP_Yield
    cols = ['MP_yield_symbol',
            'MP_yield_timestamp',
            'MP_apy_permilles']
    m = len(cols)
    N = N_days
    MP_Yield = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)

    # TR_Profit
    cols = ['TR_Profit_id',
            'TR_Profit_timestamp',
            'TR_Profit_amnt_stable',
            'TR_Profit_amnt_nls']
    m = len(cols)
    N = N_days
    TR_Profit = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)

    # TR_Rewards_Distribution
    cols = ['TR_Rewards_id',
            'TR_Rewards_Pool_id',
            'TR_Rewards_timestamp',
            'TR_Rewards_amnt_stable',
            'TR_Rewards_amnt_nls']
    m = len(cols)
    N = N_days
    TR_Rewards_Distribution = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)

    # TR_CurrentState
    cols = ['TR_timestamp',
            'TR_amnt_stable',
            'TR_amnt_nls']
    m = len(cols)
    N = N_days
    TR_CurrentState = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)

    # PL_CurrentState
    cols = ['PL_timestamp',
            'PL_pools_deposited_stable',
            'PL_pools_borrowed_stable',
            'PL_pools_yield_stable',
            'PL_LS_count_open',
            'PL_LS_count_closed',
            'PL_LS_count_opened',
            'PL_IN_LS_cltr_amnt_opened_stable',
            'PL_LP_count_open',
            'PL_LP_count_closed',
            'PL_LP_count_opened',
            'PL_OUT_LS_loan_amnt_stable',
            'PL_IN_LS_rep_amnt_stable',
            'PL_IN_LS_rep_prev_margin_stable',
            'PL_IN_LS_rep_prev_interest_stable',
            'PL_IN_LS_rep_current_margin_stable',
            'PL_IN_LS_rep_current_interest_stable',
            'PL_IN_LS_rep_principle_stable',
            'PL_OUT_LS_amnt_stable',
            'PL_native_amnt_stable',
            'PL_native_amnt_nolus',
            'PL_IN_LP_amnt_stable',
            'PL_OUT_LP_amnt_stable',
            'PL_TR_profit_amnt_stable',
            'PL_TR_profit_amnt_nls',
            'PL_TR_tax_amnt_stable',
            'PL_TR_tax_amnt_nls',
            'PL_OUT_TR_rewards_amnt_stable',
            'PL_OUT_TR_rewards_amnt_nls']
    m = len(cols)
    N = N_days
    PL_CurrentState = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)

    cols = ['platform_assets',
            'treasury_capacity',
            'cash_flow_in',
            'cash_flow_out',
            'IN_BR_counts',
            'crt_BR_counts',
            'OUT_BR_counts',
            'IN_LP_counts',
            'crt_LP_counts',
            'OUT_LP_counts',
            'pool_funds']
    m = len(cols)
    N = N_days
    MC_Output = pd.DataFrame(np.full((N, m), np.nan)*np.nan, columns=cols)
    return MP_Asset, MP_Asset_Daily, LS_Opening, LS_Closing, LS_Repayment, LS_Liquidation, LS_CurrentState_Daily, LP_Deposit, LP_Withdraw, LP_Lender_CurrentState, LP_Pool, LP_Pool_CurrentState, MP_Yield, TR_Profit, TR_Rewards_Distribution, TR_CurrentState, PL_CurrentState, MC_Output

##############################################################################
def TicTocGenerator():
    # Generator that returns time differences
    ti = 0           # initial time
    tf = time.time() # final time
    while True:
        ti = tf
        tf = time.time()
        yield tf-ti # returns the time difference

TicToc = TicTocGenerator() # create an instance of the TicTocGen generator
# This will be the main function through which we define both tic() and toc()
def toc(msg=None, dsp=True):
    # Prints the time difference yielded by generator instance TicToc
    tempTimeInterval = next(TicToc)
    if dsp:
        if msg is None: print('Elapsed time: %f seconds.\n' %tempTimeInterval, sep='')
        else:           print(msg, ': %f seconds.\n' %tempTimeInterval, sep='')
    return tempTimeInterval

def tic(msg=None):
    # Records a time in TicToc, marks the beginning of a time interval
    if msg is not None: print(msg)
    toc(dsp=False)

TicToc1 = TicTocGenerator() # create another instance of the TicTocGen generator
def toc1(msg=None, dsp=True):
    tempTimeInterval = next(TicToc1)
    if dsp:
        if msg is None: print('Elapsed time: %f seconds.\n' % tempTimeInterval, sep='')
        else:           print(msg, ': %f seconds.\n' % tempTimeInterval, sep='')
    return tempTimeInterval
def tic1(msg=None):
    if msg is not None: print(msg)
    toc1(dsp=False)

TicToc2 = TicTocGenerator() # create another instance of the TicTocGen generator
def toc2(msg=None, dsp=True):
    tempTimeInterval = next(TicToc2)
    if dsp:
        if msg is None: print('Elapsed time: %f seconds.\n' % tempTimeInterval, sep='')
        else:           print(msg, ': %f seconds.\n' % tempTimeInterval, sep='')
    return tempTimeInterval
def tic2(msg=None):
    if msg is not None: print(msg)
    toc2(dsp=False)

TicToc3 = TicTocGenerator() # create another instance of the TicTocGen generator
def toc3(msg=None, dsp=True):
    tempTimeInterval = next(TicToc3)
    if dsp:
        if msg is None: print('Elapsed time: %f seconds.\n' % tempTimeInterval, sep='')
        else:           print(msg, ': %f seconds.\n' % tempTimeInterval, sep='')
    return tempTimeInterval
def tic3(msg=None):
    if msg is not None: print(msg)
    toc3(dsp=False)

TicToc4 = TicTocGenerator() # create another instance of the TicTocGen generator
def toc4(msg=None, dsp=True):
    tempTimeInterval = next(TicToc4)
    if dsp:
        if msg is None: print('Elapsed time: %f seconds.\n' % tempTimeInterval, sep='')
        else:           print(msg, ': %f seconds.\n' % tempTimeInterval, sep='')
    return tempTimeInterval
def tic4(msg=None):
    if msg is not None: print(msg)
    toc4(dsp=False)

TicToc5 = TicTocGenerator() # create another instance of the TicTocGen generator
def toc5(msg=None, dsp=True):
    tempTimeInterval = next(TicToc5)
    if dsp:
        if msg is None: print('Elapsed time: %f seconds.\n' % tempTimeInterval, sep='')
        else:           print(msg, ': %f seconds.\n' % tempTimeInterval, sep='')
    return tempTimeInterval
def tic5(msg=None):
    if msg is not None: print(msg)
    toc5(dsp=False)

##############################################################################
