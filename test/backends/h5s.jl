using BigArrays
using BigArrays.H5sBigArrays

a = rand(200,200,10)

ba = H5sBigArray("/tmp/test.h5sbigarray"; blockSize=(256,256,32), chunkSize=(8,8,2))

ba[301:500, 201:400, 101:110] = a

b = ba[301:500, 201:400, 101:110]

@assert all(a.==b)
