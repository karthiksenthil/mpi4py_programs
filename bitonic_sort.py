from numpy import *
import sys


'''
# Serial implementation of bitonic sort

def compare_and_exchange(i,j,direction):
	if direction == (arr[i] > arr[j]):
		temp = arr[i]
		arr[i] = arr[j]
		arr[j] = temp

def bitonic_merge(low,count,direction):
	if count >= 1:
		k = count/2
		for i in range(low+k):
			compare_and_exchange(i,i+k,direction)
		bitonic_merge(low,k,direction)
		bitonic_merge(low+k,k,direction)


def bitonic_sort(low,count,direction):
	if count >= 1:
		k = count/2
		bitonic_sort(low,k,ASCENDING)
		bitonic_sort(low+k,k,DESCENDING)
		bitonic_merge(low,count,direction)



ASCENDING = True
DESCENDING = False
arr = random.randint(0,32,16) #array of size=power of 2
print "Unsorted array: "+str(arr)
bitonic_sort(0,len(arr),ASCENDING)
print "Sorted array: "+str(arr)
'''

###############################################################

# Parallel bitonic sort

from mpi4py import MPI 

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

def exchange(pe):
	global local_arr
	my_status = MPI.Status()
	sizeA = len(local_arr)
	comm.bsend(sizeA,dest=pe,tag=42)
	req = comm.Isend([local_arr,MPI.INT],dest=pe,tag=84)
	sizeB = 0
	sizeB = comm.recv(source=pe,tag=42,status=my_status)
	recvBuf = zeros(sizeB,dtype=int64)
	comm.Recv([recvBuf,MPI.INT],source=pe,tag=84,status=my_status)
	req.Wait()

	return recvBuf

def merge(data1,data2,inf):
	posA=0
	posB=0
	sizeA=len(data1)
	sizeB=len(data2)

	tmp = zeros(sizeA,dtype=int64)

	while posA+posB < sizeA:
		if inf: #increasing
			if posB < sizeB:
				if data1[posA] < data2[posB]:
					tmp[posA+posB] = data1[posA]
					posA+=1
				else:
					tmp[posA+posB] = data2[posB]
					posB+=1
			else:
				tmp[posA+posB] = data1[posA]
				posA+=1

		else:	#decreasing
			if posB < sizeB:
				if data1[sizeA-1-posA] < data2[sizeB-1-posB]:
					tmp[sizeA-1-posA-posB] = data2[sizeB-1-posB]
					posB+=1
				else:
					tmp[sizeA-1-posA-posB] = data1[sizeA-1-posA]
					posA+=1
			else:
				tmp[sizeA-1-posA-posB] = data1[sizeA-1-posA]
				posA+=1

	return tmp


def compareSplit(pe,inf):
	global local_arr
	recvBuf = exchange(pe)
	#merge the vectors recvBuf and local_arr and store result in local_arr
	if inf:
		local_arr = merge(local_arr,recvBuf,(rank<pe))
	else:
		local_arr = merge(local_arr,recvBuf,(rank>pe))


def bitToSplit(etape,inf):
	global local_arr
	pe = rank^(1<<etape)
	compareSplit(pe,inf)


def bitToMerge(n,minimum):
	# print minimum
	global local_arr
	nStage = 0
	# nStage = log_2(n)
	while (n>>nStage) > 1:
		nStage += 1

	stage = nStage-1
	while stage>=0:
		bitToSplit(stage,minimum)
		stage -= 1



def bitToSort(incr):
	global local_arr
	n = 2
	while n <= size:
		if incr: # bitonic MergeMin
			bitToMerge(n,((rank&n)==0))
		else:		 # bitonic MergeMax
			bitToMerge(n,((rank&n)>0))
		n *= 2



localSize = int(sys.argv[1]) 										#local array size
incr = sys.argv[2] 															#ascending or descending
bufSize = 3*dtype(int64).itemsize*localSize			
buff = zeros(bufSize,dtype=int64)									#buffer for send-recv operations

MPI.Attach_buffer(buff)

local_arr = random.randint(0,localSize,localSize)	# random local array

print "Before sort,Process "+str(rank)+":"+str(local_arr)

comm.Barrier()
start = MPI.Wtime()

local_arr = sort(local_arr)

if size > 1:
	bitToSort(incr)

comm.Barrier()
stop = MPI.Wtime()

if rank == 0 :
	print "Time taken:" + str(stop-start)
	f = open('gnuplot.data','a+')
	f.write(str(size)+"\t"+str(stop-start)+"\n")
	f.close()

print "After sort,Process "+str(rank)+":"+str(local_arr)


MPI.Detach_buffer()
