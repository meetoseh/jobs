import multiprocessing as mp

try:
    mp.set_start_method("spawn")
except RuntimeError:
    pass
