# -*-coding=utf-8-*-
__author__ = 'jiangdapng(asuwill.jdp@gmail.com)'

import subprocess
import multiprocessing


def run_client_process(port):
    subprocess.Popen('python client.py -p %s ' % port)


if __name__ == '__main__':

    processes = []
    for i in range(10):
        process = multiprocessing.Process(target=run_client_process, args=(str(8001 + i),))
        process.start()
        processes.append(process)


