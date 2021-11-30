import simpy
import random
import numpy as np
import scipy.stats as st
import matplotlib.pyplot as plt
from joblib import Parallel, delayed


class queuSim():
    def __init__(self, randomSeed = 42, newCustomers = 500, intervalCustomers = 2, serviceTime = 2, numSim = 100, 
    capacity = 1, arrivalDistribution = "M", serviceDistribution = "M", helpStrat = "FIFO"):
        
        
        self.newCustomers = newCustomers
        self.serviceTime = serviceTime
        self.numSim = numSim
        self.capacity = capacity
        self.intervalCustomers = intervalCustomers / capacity
        self.seed = random.seed(randomSeed)

        self.arrivalDistribution = arrivalDistribution
        self.serviceDistribution = serviceDistribution
        self.helpStrat = helpStrat


    def runSim(self, verbose=False):
        waitTimes = []
        for i in range(self.numSim):
            env = simpy.Environment()
            servers = simpy.Resource(env, self.capacity)
            # we scale the arrival rate with the number of servers, so system load is stable
            env.process(self.source(env, servers, waitTimes, verbose))
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
    
        with servers.request() as req:
            # Wait for the counter 
            yield req
    
            wait = env.now - arrive
            waitTimes.append(wait)
            # We got to the counter
            if verbose:
                print('%7.4f %s: Waited %6.3f' % (env.now, name, wait))
    
            tib = random.expovariate(1.0 / serviceTime) # markovian service rate
            yield env.timeout(tib)
            if verbose:
                print('%7.4f %s: Finished' % (env.now, name))


def plotResults(waitTimes1, waitTimes2, waitTimes4):
    fig, (ax1, ax2, ax3) = plt.subplots(1,3, sharey=True)
    fig.set_size_inches(10.5, 10.5)
    fig.suptitle(f"waiting time per customer")
    fig.supxlabel("Customer number")
    fig.supylabel("Waiting time")
    ax1.plot(np.sort(waitTimes1))
    ax1.set_title("1 server")
    ax2.plot(np.sort(waitTimes2))
    ax2.set_title("2 servers")
    ax3.plot(np.sort(waitTimes4))
    ax3.set_title("4 servers")