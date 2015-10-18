from numpy import *
import sys

'''
# Sequential Odd Even Sort

def swap(i,j):
	temp = arr[i]
	arr[i] = arr[j]
	arr[j] = temp

arr = random.randint(0,2000,2000)

# print arr
start = time.time()
for phase in range(len(arr)):
	# even phase
	if phase%2 == 0 :
		for i in range(1,len(arr),2):
			if arr[i] < arr[i-1]:
				swap(i,i-1)
	else:
		for i in range(1,len(arr)-1,2):
			if arr[i] > arr[i+1]:
				swap(i,i+1)
time_taken = time.time()-start

print "Sorted array : " + str(arr)
print "Time taken :"+ str(time_taken)
'''


# Parallel Odd Even Sort
from mpi4py import MPI

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

def merge(a,b):
	c = zeros(len(a)+len(b),dtype=int64)
	i=j=k=0
	# print a[i],b[j]

	while i<len(a) and j<len(b):
		if a[i]<=b[j]:
			c[k] = a[i]
			i+=1
		else:
			c[k] = b[j]
			j+=1
		k+=1

	while i<len(a):
		c[k] = a[i]
		i+=1
		k+=1

	while j<len(b):
		c[k] = b[j]
		j+=1
		k+=1

	return c



def get_partner(phase,rank):
	if phase%2 == 0:
		if rank%2 == 0:
			partner = rank+1
		else:
			partner = rank-1
	else:
		if rank%2 == 0:
			partner = rank-1
		else:
			partner = rank+1

	if partner < 0 or partner >= size:
		return None
	else:
		return partner

n = int(sys.argv[1])
n_per_proc = n/size

if rank == 0:
	arr = random.randint(0,2*n,n)
	print "Unsorted array: "+str(arr)
else:
	arr = zeros(n,dtype=int64)

comm.Bcast([arr,MPI.INT],root=0)

local_arr = split(arr,size)[rank]

comm.Barrier()
start = MPI.Wtime()

# Step 1. local sort
local_arr = sort(local_arr) 

# Step 2. compare and exchange operations, check get_partner for odd
# even transposition
for phase in range(size):
	partner = get_partner(phase,rank)
	if partner != None :
		comm.Send([local_arr,MPI.INT],dest=partner,tag=42)
		partner_arr = zeros(n_per_proc,dtype=int64)	
		comm.Recv([partner_arr,MPI.INT],source=partner,tag=42)
		merged = merge(local_arr,partner_arr)

		if(rank<partner):
			local_arr = merged[0:n_per_proc]
		else:
			local_arr = merged[n_per_proc:len(merged)]


comm.Barrier()
end_local = MPI.Wtime()
if rank == 0:
	f = open('gnuplot.data','a')
	f.write(str(size)+"\t"+str(end_local-start)+"\n")
	f.close()
	print "Time taken for local computations:" + str(end_local-start)

# Step 3. Gather all local arrays into final array
comm.Allgather([local_arr,MPI.INT],[arr,MPI.INT])
time_taken = MPI.Wtime() - start

if rank == 0:
	print "Sorted array : " + str(arr)
	print "Time taken after all communications:"+str(time_taken)
