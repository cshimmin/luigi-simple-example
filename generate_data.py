import luigi
import os
import numpy as np

'''example command line usage:

generate a bunch of data up front, then calculate the means
of each sample. Note that GenerateAllData and its sub tasks
don't bother running a second time when CalculateMeans is run.

  $ luigi --module generate_data GenerateAllData --n_sample 10000
  $ luigi --module generate_data CalculateMeans --n_sample 10000

or, instead of the above, just let the dependency graph resolve
itself and generate the data on depend by simply running CalculateMeans:
  $ luigi --module generate_data CalculateMeans --n_sample 10000
'''

# simple task to generate some silly toy data which is
# the sum of two poissons.
# parameters:
# mu1 - the mean of the first poisson
# mu2 - the mean of the second poisson
# n_sample - the total number of events to generate
# ratio - the ratio of poisson1 vs. poisson 2 in the data
class GenerateData(luigi.Task):
    mu1 = luigi.IntParameter()
    mu2 = luigi.IntParameter()
    ratio = luigi.FloatParameter()
    n_sample = luigi.IntParameter()

    def output(self):
        return luigi.LocalTarget('data/mu_%d_%d_r%g.npy'%(self.mu1,self.mu2,self.ratio))

    def complete(self):
        if not self.output().exists(): return False
        # output exists, load the file and see if it has the right
        # number of events
        outfile = self.output().open('r')
        d = np.load(outfile)
        return len(d) == self.n_sample

    def run(self):

        n1 = int(self.ratio*self.n_sample)
        n2 = self.n_sample-n1

        d1 = np.random.poisson(self.mu1, size=n1)
        d2 = np.random.poisson(self.mu2, size=n2)

        data = np.hstack([d1,d2])
        np.random.shuffle(data)

        outfile = self.output().open('w')
        np.save(outfile, data)
        outfile.close()

# a task which depends on the GenerateData task;
# it generates an ensemble of data with different mu2
# parameters:
# n_sample - the number of events in each sample generated
class GenerateAllData(luigi.Task):
    n_sample = luigi.IntParameter()

    def requires(self):
        tasks = []
        for mu in 10,20,30,40,50:
            tasks.append(GenerateData(mu1=10, mu2=mu, ratio=0.3, n_sample=self.n_sample))
        return tasks

    def complete(self):
        return all([t.complete() for t in self.requires()])

# a task which depends on the GenerateAllData task.
# calculates the means of all the input datasets.
# parameters:
# n_sample - the number of events of each sample to generate
class CalculateMeans(luigi.Task):
    n_sample = luigi.IntParameter()
    
    def requires(self):
        return GenerateAllData(n_sample=self.n_sample)

    def output(self):
        return luigi.LocalTarget("data/means.dat")

    def complete(self):
        return self.requires().complete()

    def run(self):
        results = []
        for t in self.requires().requires():
            f = t.output().open('r')
            d = np.load(f)
            results.append((t.mu1, t.mu2, t.ratio, d.mean()))
        results.sort()

        outfile = self.output().open('w')
        outfile.write("mu1\tmu2\tratio\tmean\n")
        for r in results:
            outfile.write('%d\t%d\t%g\t%g\n'%r)
        outfile.close()
