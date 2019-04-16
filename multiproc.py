from multiprocessing import Process, Pool
import pandas as pd
import re
from pprint import pprint as pp
from nltk.tokenize import RegexpTokenizer


tokenizer = RegexpTokenizer(r'\w+')

def preproc(s):
    """
    Function to preprocessing text data
    """
    s = s.lower().replace('ё', 'е')
    s = re.sub(r'\d+', '', s)
    s = re.sub(r'\W', ' ', s)
    s = tokenizer.tokenize(s)
    return s


def distance(a, b):
    "Calculates the Levenshtein distance between a and b."
    n, m = len(a), len(b)
    if n > m:
        # Make sure n <= m, to use O(min(n,m)) space
        a, b = b, a
        n, m = m, n
    current_row = range(n+1) # Keep current and previous row, not entire matrix
    for i in range(1, m+1):
        previous_row, current_row = current_row, [i]+[0]*n
        for j in range(1,n+1):
            add, delete, change = previous_row[j]+1, current_row[j-1]+1, previous_row[j-1]
            if a[j-1] != b[i-1]:
                change += 1
            current_row[j] = min(add, delete, change)
    return current_row[n]


file_sap = pd.read_csv('sap_hr.csv', sep=';', encoding='utf-8', error_bad_lines=False, low_memory=False)[['ФИО']]
sap = file_sap.values
sap = [preproc(i[0]) for i in sap]


def searching(i):
    pairs = []
    excep = []
    for j in sap:
        if i:
            if distance(i[0], j[0]) < 2 and len(i)>1 and len(j)>1:
                try:
                    if distance(i[1], j[1]) < 3:
                        if distance(i[2], j[2]) < 4:
                            pairs.extend([' '.join(i) + '-----' + ' '.join(j)])
                            flag = True
                        if (len(i[1]) == 1 or len(j[1]) == 1) and i[1][0] == j[1][0]:
                            if (len(i[2]) == 1 or len(j[2]) == 1) and i[2][0] == j[2][0]:
                                pairs.extend([' '.join(i) + '-----' + ' '.join(j)])
                                flag = True
                except IndexError:
                    pairs.extend([' '.join(i) + '-----' + ' '.join(j)])
                    flag = True
    if not flag:
        excep.extend([' '.join(i)])
    return pairs, excep


if __name__ == '__main__':
    file_askp = pd.read_csv('askp_hr.csv', sep=';', encoding='utf-8', error_bad_lines=False, low_memory=False)[
        ['ФИО водителя']]

    askp = file_askp.values

    askp = [preproc(i[0]) for i in askp if type(i[0]) is str]

    askp = askp[1000:1005]

    p = Pool(len(askp))
    results = p.map_async(searching, askp)
    res_pairs = [j for i in results.get() for j in i[0]]
    excep_pairs = [j for i in results.get() if i[-1] for j in i[-1]]

    pp(res_pairs)

