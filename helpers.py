import simpy
import random
import numpy as np
from joblib import Parallel, delayed


class queuSim():
    def __init__(self, randomSeed = 42, newCustomers = 500, intervalCustomers = 2, serviceTime = 2, numSim = 100, 
    arrivalDistribution = "M", serviceDistribution = "M", capacity = 1, helpStrat = "FIFO", verbose=False):
        """creates a class that runs a simulation of a queue.

        Args:
            randomSeed (int, optional): Choose a seed to have similarity between simulations. Defaults to 42.
            newCustomers (int, optional): The amount of customers that visits per simulation. Defaults to 500.
            intervalCustomers (int, optional): The average interval between customers. Defaults to 2.
            serviceTime (int, optional): The average time a customer occupies a server. Defaults to 2.
            numSim (int, optional): The amount of simulations ran. Defaults to 100.
            capacity (int, optional): The amount of servers in the system. Defaults to 1.
            arrivalDistribution (str, optional): The distribution from which the arrival intervals are drawn. Defaults to "M".
            serviceDistribution (str, optional): The distribution from which the service times are drawn. Defaults to "M".
            helpStrat (str, optional): The strategy of servers to choose which customer to help. Defaults to "FIFO".
            verbose (bool, optional): Let the program print statements for every event. WARNING: this heavily decreases performance. Defaults to False.
        """
        assert helpStrat == "SJF" or helpStrat == "FIFO", "This helpstrat has not yet been implemented"
        assert serviceDistribution == "M" or serviceDistribution == "D" or serviceDistribution  == "H", "This service distribution has not yet been implemented"
        assert arrivalDistribution == "M", "This arrival distribution has not yet been implemented"

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
        """Caller function for parallelization.

        Returns:
            list: Returns a list containing all lists with waiting times for customers per simulation
        """
        
        results = Parallel(n_jobs=8)(delayed(self.process)(i) for i in range(self.numSim))
        return results


    def process(self,i):
        """Main loop of the simulation.

        Args:
            i (int): This is only used for parallelization, should not be used

        Returns:
            list: Returns a list of waiting times of customers.
        """
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
        """Creates servers within the simpy environment.

        Args:
            env (Simpy environment): This is the environment in which the simulation is running.
            servers (Simpy resource): The avalaible servers for this simulation.
            waitTimes (list): A python list object in which the waiting times are saved.
            verbose (Bool): Choose whether or not to print status updates.

        Yields:
            environment timeout: A simpy function to progress the time when a customer arrives.
        """
        for i in range(self.newCustomers):
            c = self.customer(env, 'Customer%02d' % i, servers, waitTimes, verbose)
            env.process(c)
            t = random.expovariate(1.0 / self.intervalCustomers) # markovian arrival rate
            yield env.timeout(t)
    
    
    def customer(self, env, name, servers, waitTimes, verbose):
        """A function that uses simpy logic to make a customer look for a avalaible desk and make the time progress in the user specified way. 

        Args:
            env (Simpy environment): This is the environment in which the simulation is running.            
            name (String): A name for the current customer. Only used when verbose is on.   
            servers (Simpy resource): Simpy resources that simulate the servers.
            waitTimes (list): A python list object to save the waiting times.
            verbose (Bool): Choose whether or not to print status updates. 

        Yields:
            environment timeout: A simpy function to progress the time while the customer is being helped.
        """
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
            tib = random.expovariate(1.0 / self.serviceTime) 

        if self.helpStrat == "SJF":
            # with servers.request(priority = int(1/tib*1000)) as req:
            with servers.request(priority = tib) as req:
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
