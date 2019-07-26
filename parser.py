import json
import multiprocessing
from itertools import chain


class Parser:

    def __init__(self, dataframe, *actions):
        self.dataframe = dataframe
        self.actions = actions

    def helper(self, idx0, idxf):
        result = []
        for datapoint in chain(*self.dataframe.loc[idx0:idxf, 'actions'].apply(json.loads)):
            if datapoint['action'] in self.actions:
                result.append(datapoint)
        return result

    def run(self, P=1):
        N = self.dataframe.shape[0]
        if P > 1:
            with multiprocessing.Pool(processes=P) as pool:
                n = N // P
                results = pool.starmap(self.helper, ([n*i, min(n*(i+1)-1, N)] for i in range(P)))
        else:
            results = [self.helper(0, N)]
        return list(chain(*results))