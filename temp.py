#!/usr/bin/env python
# encoding: utf-8
# AUTHOR: XIANWU LIN
# EMAIL: linxianwu@intra.nsfocus.com
# TIME: 2016/2/2 15:30



def get_remove_list(dict1, dict2):
    return list(set(dict1.keys()).difference(dict2.keys()))

def get_update_dict(dict1, dict2):
    update_dict = dict()

    for item in dict2.items():
        k,v = item
        if dict1.has_key(k) and dict1[k] == v:
            pass
        else:
            update_dict[k] = v

    return update_dict

def main():
    dict1 = {"a" : 1,
         "b" : 2,
         "c" : 3,
    }

    dict2 = {
        "b" : 2,
        "c" : 4,
        "d" : 5
    }

    print get_remove_list(dict1, dict2)
    print get_update_dict(dict1, dict2)

class Main():
    def __init__(self):
        pass


if __name__ == "__main__":
    main()