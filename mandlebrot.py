from numpy import *
from matplotlib import pyplot
import time

'''
# Sequential mandlebrot code

def mandlebrot(x,y,maxit):
	c = x + y*1j
	z = 0 + 0j
	it = 0

	while abs(z) < 2 and it < maxit:
		z = z**2 + c
		it += 1

	return it

# print mandlebrot(1,1,127)
start = time.time()
x1,x2 = -2.0,1.0
y1,y2 = -2.0,2.0

nx,ny = 100,150

c = zeros((nx,ny),dtype='i')

dx = (x2-x1)/nx
dy = (y2-y1)/ny

for i in range(nx):
	y = y1 + i*dx
	for j in range(ny):
		x = x1 + j*dy
		c[i,j] = mandlebrot(x,y,127)

end = time.time()
print "Total time taken: "+str(end-start)

pyplot.imshow(c, aspect='equal')
pyplot.spectral()
pyplot.show()
'''


# Parallel mandlebrot code
from mpi4py import MPI

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

def mandlebrot(x,y,maxit):
	c = x + y*1j
	z = 0 + 0j
	it = 0

	while abs(z) < 2 and it < maxit:
		z = z**2 + c
		it += 1

	return it

start = MPI.Wtime()

x1,x2 = -2.0,1.0
y1,y2 = -2.0,2.0

nx,ny = 1000,1500

strip_size = nx/size
dx = (x2-x1)/nx
dy = (y2-y1)/ny

c = zeros((nx,ny),dtype='i')

# without load balancing
# for i in range(rank*strip_size,rank*strip_size+strip_size):
# 	y = y1 + i*dx
# 	for j in range(ny):
# 		x = x1 + j*dy
# 		c[i,j] = mandlebrot(x,y,127)

# with load balancing by cyclic distribution
i = rank
while i < nx:
	y = y1 + i*dx
	for j in range(ny):
		x = x1 + j*dy
		c[i,j] = mandlebrot(x,y,127)
	i += size

end_local = MPI.Wtime()
print "Total time taken for local calculation by process "+str(rank)+" : "+str(end_local-start)


comm.Allreduce(MPI.IN_PLACE,c,op=MPI.MAX)

if rank == 0:
	# print c
	end_comm = time.time()
	print "Total time taken for all communications : "+str(end_comm-start)
	pyplot.imshow(c, aspect='equal')
	pyplot.spectral()
	pyplot.show()
