from ptvp3 import P3terpreter
import sys
import json


p3p = P3terpreter.from_param_dict(json.load(open(sys.argv[1])))
p3p.start()
