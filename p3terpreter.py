from pers3cluster import LimitedCluster
from persistence3 import Persistence3
from typing import List, Optional
import threading
from time import sleep, time


"""
Example:
p3env1 = P3Env("abc", 2)
p3session1 = P3Session(p3env1)
p3session1.start()
p3session1.wait_running()

env = p3session1.env

print(dict(env.items()))

p3session1.stop()
"""


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
        for env_i in self.env_list:
            env_i.save()

    def wipe(self, skip=False):
        """

        :param skip: whether skip Y/n question
        """
        if not skip:
            if input("proceed wiping? Y/n") not in ["Y", "y", ""]:
                return -1
        self.env.clear()
        return 0


class P3Session(threading.Thread):
    def __init__(self, p3env: P3Env, do_print: bool = True, delta_tick: float = 2, save_mu: int = 5):
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
            def __init__(self, p3s: P3Session):
                super().__init__()
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
                self.p3s.p3env.save_all()
                if self.p3s.do_print:
                    self.p3s.log("saved all")

        self.autoSave = AutoSave(self)

    def log(self, s: str):
        """
prints output (s) as standard form time+filename+s
        """
        print("[{}][{}] {}".format(time(), self.p3env.filename, s))

    def run(self) -> None:
        self.p3env.init_env()
        self.env = self.p3env.env
        self.running = True
        self.autoSave.start()
        while self.running:
            sleep(self.delta_tick)
        while self.autoSave.isAlive():
            self.log("stopping autoSave")
            self.autoSave.join(self.delta_tick)

    def stop(self):
        self.running = False

    def wait_running(self, d_time: float = 0.0001):
        while not self.running:
            sleep(d_time)
