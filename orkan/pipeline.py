from multiprocessing import cpu_count
from multiprocessing.queues import SimpleQueue
from concurrent.futures import ProcessPoolExecutor


VERBOSE = False
verbose_output = SimpleQueue()

def _log(msg):
    verbose_output.put(msg)
    print msg

signals = SimpleQueue()  # Queue of signals for the manager
queues = []  #


def _init_queues(n):
    global verbose_output
    verbose_output = SimpleQueue()
    global signals
    signals = SimpleQueue()
    global queues
    queues = [SimpleQueue() for _ in range(n)]


def _spout(f, i, j):
    """
    Wrapper for a spout function. Calls the spout function with
    a callback function, which is used by the spout function to
    put items into the first queue.
    """
    if VERBOSE:
        _log("SPOUT %s.%s: START" % (i, j))
    signals.put(("START", i))
    def add(n):
        if VERBOSE:
            _log("SPOUT %s.%s: SPAWN %s" % (i, j, n))
        queues[0].put(n)
    f(add)
    if VERBOSE:
        _log("SPOUT %s.%s: END" % (i, j))
    signals.put(("STOP", i))


def _bolt(f, i, j):
    """
    Wrapper for a bolt function. Takes elements from the i'th
    queue, calls f(element) and puts the result on the i+1'th
    queue.
    """
    if VERBOSE:
        _log("BOLT %s.%s: START" % (i, j))
    signals.put(("START", i))
    for n in iter(queues[i].get, 'STOP'):
        if VERBOSE:
            _log("BOLT %s.%s: PROCESS %s" % (i, j, n))
        r = f(n)
        queues[i + 1].put(r)
    queues[i].put("STOP")  # repeat Sentinel for sister processes
    if VERBOSE:
        _log("BOLT %s.%s: END" % (i, j))
    signals.put(("STOP", i))


def _vent(f, i, j):
    """
    Wrapper for a vent function. Takes elements from the i'th
    queue and calls f(element).
    """
    if VERBOSE:
        _log("VENT %s.%s: START" % (i, j))
    signals.put(("START", i))
    for n in iter(queues[i].get, 'STOP'):
        if VERBOSE:
            _log("VENT %s.%s: PROCESS %s" % (i, j, n))
        f(n)
    queues[i].put("STOP")  # repeat Sentinel for sister processes
    if VERBOSE:
        _log("VENT %s.%s: END" % (i, j))
    signals.put(("STOP", i))


def _manager(jobs):
    """
    Manages the
    """
    if VERBOSE:
        _log("MANAGER: START")
    for signal, i in iter(signals.get, 'STOP'):
        if VERBOSE:
            _log("MANAGER: PROCESS %s %s" % (signal, i))
        if signal == "START":
            pass
        elif signal == "STOP":
            jobs[i + 1] -= 1
            if jobs[i + 1] == 0 and i + 1 < len(queues):
                if VERBOSE:
                    _log("MANAGER: END QUEUE %s" % (i + 1))
                queues[i + 1].put("STOP")
        else:
            ValueError("Got unknown signal: " + signal)
        if not any(jobs):
            signals.put("STOP")
    if VERBOSE:
        _log("MANAGER: STOP")


class Pipeline(object):
    """
    A pipeline is a chain of computations, in which the output of
    one computation is the input to the next. The processing of
    different modules in the pipeline can be parallelized, as well
    as multiple workers for each module. Use start() to start the
    execution of the processing pipeline.

    Parameters
    ----------

    spouts : list of (function, int)-tuples
        Spouts are the processes which feed elements into the
        Pipeline. They are defined as functions accepting a callback
        function, which is used to pass an element into the pipeline.

    bolts : list of (function, int)-tuples
        Bolts are the processes inside the pipeline which do the
        further processing. They are defined as functions which accept
        an element from the previous processing step and pass on
        (return) an element to the next module in the pipeline.

    vent : (function, int)-tuple, optional
        Vents are an optional type of bolts, which won't pass on their
        result and thus are the last stage in a pipeline. This is
        useful, if you don't care about the actual return value of the
        pipeline computation, but rather want to send or save the result
        somewhere else.
    """

    def __init__(self, spouts, bolts, vent=None):

        self.spouts = spouts
        self.bolts = bolts
        self.vent = vent

        self.no_processes = [n for _, n in spouts + bolts]
        if vent:
            self.no_processes.append(vent[1])

        if any([n > cpu_count() for n in self.no_processes]):
            raise ValueError("Number of jobs for each element should not"
                             " exceed number of CPUs (%s)." % cpu_count())

    def start(self, n_jobs=None, verbose=False):
        """
        Starts the pipeline with maximal `n_jobs` processes running
        in parallel.

        Parameters
        ----------
        n_jobs : int, optional
            Number of parallel processes to be started. Defaults to
            the number of CPUs. One process will always be added for
            the internal pipeline queue manager.

        verbose: bool, optional
            If true activates verbose output (logging).

        Returns
        -------
        results : list or None
            Returns a list of elements processed by the pipeline.
            In case of using a vent no results are collected, so this
            function will return None.
        """
        if verbose:
            global VERBOSE
            VERBOSE = True

        _init_queues(len(self.bolts) + 1 + int(bool(self.vent)))
        if not n_jobs:
            n_jobs = cpu_count()

        # add one job extra for queue manager
        with ProcessPoolExecutor(max_workers=n_jobs + 1) as executor:
            m = executor.submit(_manager, self.no_processes)
            while not m.running():
                pass

            i = -1
            for s, n in self.spouts:
                for j in range(n):
                    executor.submit(_spout, s, i, j)

            for b, n in self.bolts:
                i += 1
                for j in range(n):
                    executor.submit(_bolt, b, i, j)

            if self.vent:
                v, n = self.vent
                i += 1
                for j in range(n):
                    executor.submit(_vent, v, i, j)

        if not self.vent:
            if VERBOSE:
                _log("Compiling results...")
                verbose_output.put("STOP")
                print "--------------------"
                for n in iter(verbose_output.get, 'STOP'):
                    print n

            res = []
            for n in iter(queues[-1].get, 'STOP'):
                res.append(n)

            return res
