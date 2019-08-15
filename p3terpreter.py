"""Persistence protocol v3
"""

from pers3cluster import LimitedCluster
from persistence3 import Persistence3
from typing import List, Optional
import threading
from time import sleep, time
import logging


class P3Env:
    def __init__(self, filename: str, cluster_lev: int):
        self.filename = filename
        self.env_list: List[Persistence3] = []
        self.cluster_lev = cluster_lev
        self.size = 1 << cluster_lev
        self.env: Optional[LimitedCluster] = None

    def init_env(self):
        """
        initializes files
        initializes pickle'd objects (.o) in persistence
        initializes cluster with persistence
        cleans env cluster
        """
        self.env_list: List[Persistence3] = [Persistence3(self.filename + '_' + str(_i)) for _i in range(self.size)]
        for env_i in self.env_list:
            if type(env_i.o) != dict:
                env_i.o = {}
        self.env = LimitedCluster([env_i.o for env_i in self.env_list])
        self.env.clean()

    def save_all(self):
        """
        saves every its persistence
        :return:
        """
        for env_i in self.env_list:
            env_i.save()

    def wipe(self, skip=False):
        """
        clears env
        :param skip: whether skip Y/n question
        """
        if not skip:
            if input("proceed wiping? Y/n") not in ["Y", "y", ""]:
                return -1
        self.env.clear()
        return 0


class P3Session(threading.Thread):
    def __init__(self, p3env: P3Env, do_print: bool = True, delta_tick: float = 2, save_mu: int = 5,
                 daemon: bool = False):
        super().__init__()
        self.save_mu = save_mu
        self.delta_tick = delta_tick
        self.do_print = do_print
        self.p3env = p3env
        self.running = False
        self.env = p3env.env

        class AutoSave(threading.Thread):
            """
            Thread responsible for autoSave while running
            """
            def __init__(self, p3s: P3Session,
                         daemon_save: bool = False):
                super().__init__(daemon=daemon_save)
                self.p3s = p3s

            def run(self) -> None:
                while self.p3s.running:
                    for env_i in self.p3s.p3env.env_list:
                        if not self.p3s.running:
                            break
                        env_i.save()
                        if self.p3s.do_print:
                            self.p3s.log("saved #{}".format(env_i.filename))
                        for _ in range(self.p3s.save_mu):
                            if not self.p3s.running:
                                break
                            sleep(self.p3s.delta_tick)

        self.autoSave = AutoSave(self, daemon)

    def log(self, s: str):
        """
prints output (s) as standard form time+filename+s
        """
        logging.info("[{}][{}] {}".format(time(), self.p3env.filename, s))

    def run(self) -> None:
        self.p3env.init_env()
        self.env = self.p3env.env
        self.running = True
        self.autoSave.start()
        while self.running:
            self.running = threading.main_thread().isAlive()
            sleep(self.delta_tick)
        self.p3env.save_all()
        if self.do_print:
            self.log("saved all")
        self.stop_seq()

    def stop_seq(self):
        while self.autoSave.isAlive():
            self.log("stopping autoSave")
            self.autoSave.join(self.delta_tick)

    def __del__(self):
        self.stop_seq()

    def stop(self):
        self.running = False

    def wait_running(self, d_time: float = 0.0001):
        while not self.running:
            sleep(d_time)

    def start_wait(self):
        self.start()
        self.wait_running()
