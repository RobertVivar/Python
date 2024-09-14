import shutil
import os


def move_process(cn_like, cn_origin, cn_target):

    for file in os.listdir(cn_origin):
        if cn_like in file:
            shutil.move(cn_origin+file, cn_target+file)

    return True
