=====
Orkan
=====

Orkan is a pipeline parallelization library, written in Python.

Making use of the multicore capabilities of ones machine in
Python is often not as easy as it should be. Orkan aims to
provide a plain API to utilze those underused CPUs of yours
in cases you need some extra horse power for your computation.

Code Repo: https://github.com/tobigue/Orkan


Pipelines
=========

A pipeline is a chain of computations, in which the output of
one computation is the input to the next. Orkan allows pipeline
processing of a finite number of elements, but also the processing
of an infinite stream of elements. The processing of different
modules in the pipeline can be parallelized, as well as multiple
workers for each module.

Taking its cue from the terminology of `Storm <https://github.com/nathanmarz/storm/wiki/Concepts>`_,
Orkan adopts the concept of spouts and bolts, adding vents. In Orkan:

**Spouts** are the processes which feed elements into the Pipeline.
They are defined as functions accepting a callback function, which
is used to pass an element into the pipeline. Examples for spouts
are functions listening for input via HTTP requests, crawling the
internet, reading large files and sending off chunks for further
processing or just feeding the elements of an iterable into the pipeline::

    big_numbers = [
        112272535095293,
        112582705942171,
        112272535095293,
        115280095190773,
        115797848077099,
        1099726899285419] * 5

    def put_primes_spout(callback):
        for n in big_numbers:
            callback(n)

**Bolts** are the processes inside the pipeline which do the further
processing. They are defined as functions which accept an element from
the previous processing step and pass on a (possibly modified) element
to the next module in the pipeline (or the list of results)::

    def is_prime_bolt(n):
        """From http://docs.python.org/dev/library/concurrent.futures.html"""
        if n % 2 == 0:
            return n, False
        sqrt_n = int(math.floor(math.sqrt(n)))
        for i in range(3, sqrt_n + 1, 2):
            if n % i == 0:
                return n, False
        return n, True

**Vents** are an optional type of bolts, which won't pass on their
result and thus are the last stage in a pipeline. This is useful,
if you don't care about the actual return value of the pipeline
computation, but rather want to send or save the result somewhere
else. You should definitely use this in the case of an infinite
stream of input data, as otherwise you probably will run out of
memory at some point::

    def send_result_vent(n):
        number, is_prime = n
        requests.post("http://127.0.0.1", data={number: is_prime})

Note that spouts, bolts and vents will be started in seperate
python processes. That means, their in- and output elements have
to be *pickable* and they should *not interact with non-threadsafe
elements* in the main process. The passing of elements between the
different modules of the pipeline is realized using threadsafe Queues.


Usage
-----

This is how you set up and start a simple pipeline using the spout
and bolt defined above::

    from orkan import Pipeline

    pipeline = Pipeline([(put_primes_spout, 1)], [(is_prime_bolt, 2)])
    result = pipeline.start()

The pipeline is defined by passing a list of spouts and a list of
bolts (and optionally a vent, see examples). Each element in a list
is a tuple of the function to be executed and the number of workers
to be spawned for this function. Note that if you run more than one
worker for a function the order of result elements might not correspond
to the order of the respective input elements. If you need to relate
the result elements to the input elements, you should pass the input
elements along the pipeline (e.g. by returning a tuple in each bolt).

By default a pipeline is started with maximal n parallel processes,
where n is the number of CPUs in your machine. That means, in the
example above on a dual core machine at first one spout and one bolt
are running in parallel. As soon as the spout finishes an additional
bolt worker is spawned. On a quad core machine all three workers will
run in parallel from the beginning.

You can change this by passing a value for ``n_jobs`` to ``start()``::

    # this example corresponds to non-parallel processing
    pipeline = Pipeline([(put_primes_spout, 1)], [(is_prime_bolt, 1)])
    result = pipeline.start(n_jobs=1)

Note that in case of an infinite input stream of data you will need
at least one worker for every spout/bolt/vent, as no worker will ever
finish and thus won't free a slot for a new worker further down in the
pipeline.

You should test your spouts/bolts/vents before using in the pipeline,
as error messages are not always propagated back to the main process.


Examples
========

The examples will use the following simple spout/bolts/vent::

    def s(callback):
        """Simple spout that puts some random numbers into the Pipeline."""
        for _ in range(10):
            n = int(random.random() * 1000000)
            callback(n)

    def b1(n):
        """Simple bolt that doubles the passed element."""
        return n * 2

    def b2(n):
        """Simple bolt that halves the passed element."""
        return n / 2

    def v(n):
        """Simple vent that prints the result."""
        print n


Finite input
------------

Non-parallel processing::

    pipeline = Pipeline([(s, 1)], [(b1, 1), (b2, 1)])
    results = pipeline.start(n_jobs=1)

    """
        s1
        |
        b1
        |
        b2
        |
        result
    """

Parallel processing of pipeline modules::

    pipeline = Pipeline([(s, 1)], [(b1, 1), (b2, 1)])
    results = pipeline.start(n_jobs=4)

        s----b1----b2
                   |
                   result

Parallel workers for the b1 bolt::

    pipeline = Pipeline([(s, 1)], [(b1, 2), (b2, 1)])
    results = pipeline.start(n_jobs=4)

    """
           .-b1-------.
        s--|          |--b2
           '-------b1-'   |
                          result
    """

More workers than processes (b2 workers will wait for spouts to finish)::

    pipeline = Pipeline([(s, 2)], [(b1, 2), (b2, 2)])
    results = pipeline.start(n_jobs=4)

    """
        s-------.  .-b1-------.
                |--|          |-+
              s-'  '-------b1-' |
      .-b2-------.              |
    +-|          |--------------+
    | '-------b2-'
    |
    result
    """


Infinite Input Stream
---------------------

Endless stream of input data done right::

    def s2(callback):
        """Simple spout that produces an infinite stream of random numbers."""
        while 1:
            n = int(random.random() * 1000000)
            callback(n)

    pipeline = Pipeline([(s2, 1)], [(b1, 1), (b2, 1)], vent=(v, 1))
    results = pipeline.start(n_jobs=4)

    """
        s2---b1----b2----v
    """

Endless stream of input data done wrong (vent and one b2 worker will never start)::

    pipeline = Pipeline([(s, 1)], [(b1, 2), (b2, 2)], vent=(v, 1))
    results = pipeline.start(n_jobs=4)

    """
           .-b1-------.
        s2-|          |--b2---
           '-------b1-'
    """


Tests
=====

Testing requires having the nose library (`pip install nose`).
After installation, the package can be tested by executing from
outside the source directory::

    nosetests orkan --exe -v


Known Issues
============

* Does not work on Windows
