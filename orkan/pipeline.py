from multiprocessing import cpu_count
from multiprocessing.queues import SimpleQueue
from concurrent.futures import ProcessPoolExecutor


VERBOSE = False
verbose_output = SimpleQueue()


def _log(msg):
    if VERBOSE:
        verbose_output.put(msg)
        print msg

signals = SimpleQueue()  # Queue of signals for the manager
queues = []
sentinel = "<QueueSentinel>"


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
    _log("SPOUT %s.%s: START" % (i, j))
    signals.put(("START", i))

    def add(n):
        _log("SPOUT %s.%s: SPAWN %s" % (i, j, n))
        queues[0].put(n)

    f(add)
    _log("SPOUT %s.%s: END" % (i, j))
    signals.put(("STOP", i))


def _bolt(f, i, j):
    """
    Wrapper for a bolt function. Takes elements from the i'th
    queue, calls f(element) and puts the result on the i+1'th
    queue.
    """
    _log("BOLT %s.%s: START" % (i, j))
    signals.put(("START", i))

    def add(n):
        _log("BOLT %s.%s: PASS %s" % (i, j, n))
        queues[i + 1].put(n)

    for n in iter(queues[i].get, sentinel):
        _log("BOLT %s.%s: PROCESS %s" % (i, j, n))
        try:
            f(n, add)
        except TypeError:
            add(f(n))

    queues[i].put(sentinel)  # repeat Sentinel for sister processes
    _log("BOLT %s.%s: END" % (i, j))
    signals.put(("STOP", i))


def _manager(jobs):
    """
    Manages the queues.
    """
    _log("MANAGER: START")
    for signal, i in iter(signals.get, sentinel):
        _log("MANAGER: PROCESS %s %s" % (signal, i))
        if signal == "START":
            pass
        elif signal == "STOP":
            jobs[i + 1] -= 1
            if jobs[i + 1] == 0:
                _log("MANAGER: END QUEUE %s" % (i + 1))
                queues[i + 1].put(sentinel)
        else:
            ValueError("Got unknown signal: " + signal)
        if not any(jobs):
            signals.put("STOP")
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
    """

    def __init__(self, spouts, bolts):

        self.spouts = spouts
        self.bolts = bolts
        self.no_processes = [n for _, n in spouts + bolts]

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
        results : list
            Returns a list of elements processed by the pipeline.
        """
        if verbose:
            global VERBOSE
            VERBOSE = True

        _init_queues(len(self.bolts) + 1)
        if not n_jobs:
            n_jobs = cpu_count()

        # add one job extra for queue manager
        with ProcessPoolExecutor(max_workers=n_jobs + 1) as executor:
            m = executor.submit(_manager, self.no_processes)
            while not m.running():
                pass

            for s, n in self.spouts:
                for j in range(n):
                    executor.submit(_spout, s, -1, j)

            for i, (b, n) in enumerate(self.bolts):
                for j in range(n):
                    executor.submit(_bolt, b, i, j)

            for n in iter(queues[-1].get, sentinel):
                _log("YIELD %s" % n)
                yield n

        if VERBOSE:
            _log("Compiling results...")
            verbose_output.put(sentinel)
            print "--------------------"
            for n in iter(verbose_output.get, sentinel):
                print n
