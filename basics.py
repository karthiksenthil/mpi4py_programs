from mpi4py import MPI
from numpy import *

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()


# simple send and recv
if rank == 0:
	print "I'm master process"
	data = {'a':97,'b':98,'c':99}
	comm.send(data, dest=2, tag=42)
elif rank == 2:
	print "I'm chosen receiver"
	data = comm.recv(source=0,tag=42)
	print data
else:
	print "I'm a normal receiver"


#broadcast
if rank == 0:
	data = [2,4,6]
else:
	data = []

data = comm.bcast(data,root=0)
print "I'm process "+str(rank)+" and my data :"+str(data)



#scatter data
if rank == 0:
	data = array([0,1,4,9,16,25,36,49,64])
else:
	data = None

data_local = array([0,0])

data = comm.Scatter(data,data_local,root=0)

print rank,data_local



#gather data
data = rank**2
data = comm.gather(data,root=0)

if rank == 0:
	print rank, data
else:
	print rank, data



#reduce data
data = rank**2
data = comm.reduce(data,op=MPI.MAX,root=0)

if rank == 0:
	print rank, data
else:
	print rank, data