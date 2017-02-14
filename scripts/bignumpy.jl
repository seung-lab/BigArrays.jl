using PyCall
@pyimport numpy


a = rand(3,4,5)
b = PyCall.array2py(a)
# c = PyCall.py2array(Array{Float64,3}, b)


using BigArrays
using BigArrays.AlignedBigArrays
ba = AlignedBigArray("/mnt/data01/datasets/zebrafish/4_aligned/registry.txt");
@assert AlignedBigArray <: AbstractArray
nba = PyCall.array2py(ba, 3, )
