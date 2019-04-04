import random
import numpy as np
import math
import sys
sys.path.append("..")
import loadenv as le

# 模拟退火

# 读取城市之间的距离
location = np.loadtxt(le.pl() + '/input/city.data')
N, dim = location.shape
# 计算两两城市之间的距离矩阵，注意这里强行增大对角上的数为了避免训练过程自己走向自己的问题
dis_all = sys.maxsize * np.eye(N) + np.array([np.sqrt(np.sum(np.square(location[x] - location[y]))) for x in range(N) for y in range(N)]).reshape(N, N)
# 初始化路径矩阵
path = np.eye(N, k=1 - N, dtype=int) + np.eye(N, k=1, dtype=int)
# 初始化距离
dis_cal = np.sum(np.multiply(dis_all, path))
# 训练
temperature = 10000
while(temperature > 0.00001):
    i, j = random.sample(range(N), 2)
    path[[i, j], :] = path[[j, i], :]
    dis_egy = np.sum(np.multiply(dis_all, path))
    d_egy = dis_egy - dis_cal
    p = 1 if d_egy < 0 else math.exp(-d_egy / temperature)
    if(random.random() < p):
        dis_cal = dis_egy
    else:
        path[[i, j], :] = path[[j, i], :]
    # 温度作用：控制循环数量+偏移概率
    temperature *= 0.99
# 输出
print(dis_cal, "\n", path)

