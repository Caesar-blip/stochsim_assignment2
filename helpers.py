import simpy
import random
import numpy as np
import scipy.stats as st
import matplotlib.pyplot as plt
from joblib import Parallel, delayed


class queuSim():
    def __init__(self, randomSeed = 42, newCustomers = 500, intervalCustomers = 2, serviceTime = 2, numSim = 100, 
    capacity = 1, arrivalDistribution = "M", serviceDistribution = "M", helpStrat = "FIFO", verbose=False):
        assert helpStrat == "SJF" or helpStrat == "FIFO", "This helpstrat has not yet been implemented"
        assert serviceDistribution == "M" or serviceDistribution == "D" or serviceDistribution  == "H", "This service distribution has not yet been implemented"

        self.newCustomers = newCustomers
        self.serviceTime = serviceTime
        self.numSim = numSim
        self.capacity = capacity
        # we scale the arrival rate with the number of servers, so system load is stable
        self.intervalCustomers = intervalCustomers / capacity
        self.seed = random.seed(randomSeed)

        self.arrivalDistribution = arrivalDistribution
        self.serviceDistribution = serviceDistribution
        self.helpStrat = helpStrat
        self.verbose = verbose


    def runSim(self):
        waitTimes = Parallel(n_jobs=8)(delayed(self.process)(i) for i in range(self.numSim))
        return waitTimes


    def process(self,i):
        waitTimes = []
        env = simpy.Environment()
        if self.helpStrat == "SJF":
            servers = simpy.PriorityResource(env, self.capacity)
        else:
            servers = simpy.Resource(env, self.capacity)
        
        env.process(self.source(env, servers, waitTimes, self.verbose))
        env.run()

        return waitTimes


    def source(self, env, servers, waitTimes, verbose):
        """Source generates customers randomly"""
        for i in range(self.newCustomers):
            c = self.customer(env, 'Customer%02d' % i, servers, self.serviceTime, waitTimes, verbose)
            env.process(c)
            t = random.expovariate(1.0 / self.intervalCustomers) # markovian arrival rate
            yield env.timeout(t)
    
    
    def customer(self, env, name, servers, serviceTime, waitTimes, verbose):
        """Customer arrives, is served and leaves."""
        arrive = env.now
        if verbose:
            print('%7.4f %s: Here I am' % (arrive, name))

        if self.serviceDistribution == "D":
            tib = self.serviceTime
        elif self.serviceDistribution == "H":
            if np.random.random() < 0.25:
                tib = random.expovariate(1/5)
            else:
                tib = random.expovariate(1/1)
        else:
            # markovian service rate
            tib = random.expovariate(1.0 / serviceTime) 

        if self.helpStrat == "SJF":
            with servers.request(priority = int(1/tib*1000)) as req:
            # Wait for the counter 
                yield req
                wait = env.now - arrive
                waitTimes.append(wait)
                # We got to the counter
                if verbose:
                    print('%7.4f %s: Waited %6.3f' % (env.now, name, wait))
                yield env.timeout(tib)
                if verbose:
                    print('%7.4f %s: Finished' % (env.now, name))
        else:
            with servers.request() as req:
                # Wait for the counter 
                yield req

                wait = env.now - arrive
                waitTimes.append(wait)
                # We got to the counter
                if verbose:
                    print('%7.4f %s: Waited %6.3f' % (env.now, name, wait))

                yield env.timeout(tib)
                if verbose:
                    print('%7.4f %s: Finished' % (env.now, name))
