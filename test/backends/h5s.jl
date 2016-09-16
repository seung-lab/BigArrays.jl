using BigArrays
using BigArrays.H5sBigArrays

info("\n test 3D image reading and saving...")
H5sDir = "/tmp/test.h5sbigarray.img"
a = rand(UInt8, 200,200,10)
ba = H5sBigArray(H5sDir; blockSize=(256,256,32), chunkSize=(8,8,2))
ba[301:500, 201:400, 101:110] = a
b = ba[301:500, 201:400, 101:110]
@assert all(a.==b)
rm(H5sDir; recursive=true)


# test affinity map
info("\n test affinity map reading and saving...")
H5sDir = "/tmp/test.h5sbigarray.aff"
a = rand(Float32, 200,200,10,3)
ba = H5sBigArray(H5sDir; blockSize=(256,256,32), chunkSize=(8,8,2))
ba[301:500, 201:400, 101:110, :] = a
b = ba[301:500, 201:400, 101:110, :]

@show size(a)
@show size(b)
@assert all(a.==b)
rm(H5sDir; recursive=true)
