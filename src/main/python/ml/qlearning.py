import random as rd
import numpy as np
import math
import sys
from numpy import dtype
sys.path.append("..")
import loadenv as le

# q-learning

r_all = np.loadtxt(le.pl() + '/input/room.data')
print(r_all)
N, N = r_all.shape
tar = r_all.max()
print(tar)
q_all = np.zeros((N, N), dtype=int)

for tik in range(0, 100000):
    (i, j) = (rd.randint(0, N - 1), rd.randint(0, N - 1))
    q_all[i][j] = tar if r_all[i][j] == tar else r_all[i][j] + 0.8 * max(q_all[:, i])

print(q_all)
