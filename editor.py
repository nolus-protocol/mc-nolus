import time
from multiprocessing import Pool

work = [["W", 5], ["X", 2], ["Y", 4], ["Z", 3]]

def worker(work_data):
    print(" Process %s waiting %s seconds" % (work_data[0], work_data[1]))
    time.sleep(int(work_data[1]))
    print(" Process %s Finished." % work_data[0])

def poolHandler():
    p = Pool(2)
    p.map(worker, work)

if __name__ == '__main__':
    poolHandler()