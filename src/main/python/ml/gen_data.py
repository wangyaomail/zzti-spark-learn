import random
import numpy as np
import math
import sys
import matplotlib.pyplot as plt
sys.path.append("..")
import loadenv as le

def gen_beibao():
    # 生成背包问题模拟数据
    city_num = 10
    max_way_for_one_city = 3

    way_fin = np.zeros([city_num, city_num], dtype=int)
    for tik in range(0, max_way_for_one_city):
        way_ran = np.eye(city_num, dtype=int)
        np.random.shuffle(way_ran)
        way_fin += way_ran
    np.where(way_fin < 1, way_fin, 1)  # 去除有长度是2的way，其实也不需要有
    way_idx = np.nonzero(way_fin)
    way_zip = np.array(list(zip(way_idx[0], way_idx[1])))
    print(way_zip)
    np.savetxt(le.pl() + '/input/city.data', way_zip)
    plt.scatter(way_zip[:, 0], way_zip[:, 1])
    plt.show()

if __name__ == "__main__":
    gen_beibao();
