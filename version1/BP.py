# -*- coding: utf-8 -*-
"""
Created on Tue Oct  19 14:36:17 2018

@author: Sam
"""


def cc(startnumb,endnumb):
    initnumb = 0
    while(startnumb<=endnumb):
        startnumb = startnumb * 1.1
        initnumb = initnumb + 1
    print(initnumb)
    
    
cc(4,8)
