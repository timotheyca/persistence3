from .p3terpreter import *


def fast_start(name: str, cluster_lev: int = 2):
    p3env1 = P3Env(name, cluster_lev)
    session1 = P3Session(p3env1)
    session1.start_wait()
    env1 = session1.env
    return env1, session1
