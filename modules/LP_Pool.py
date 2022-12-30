import pandas as pd

def LP_pool_gen(args):
    list_id =["pid" + str(sub) for sub in range(100,100+len(args["Pool_Assets"]))]
    lpp = pd.DataFrame({"LP_Pool_id":list_id,"LP_symbol":args["Pool_Assets"]})
    return lpp