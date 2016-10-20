using BigArrays.GCSBigArrays

info("\n test 3D image reading and saving...")
dir = "gs://jpwu/test.bigarray.img/"
a = rand(UInt8, 200,200,10)
ba = GCSBigArray(dir)

info("test saving ...")
@time ba[301:500, 201:400, 101:110] = a
info("test reading ...")
@time b = ba[301:500, 201:400, 101:110]
@assert all(a.==b)

# test affinity map
info("\n test affinity map reading and saving...")
dir = "gs://jpwu/test.bigarray.aff/"
a = rand(Float32, 200,200,10,3)
ba = GCSBigArray(H5sDir; blockSize=(256,256,32), chunkSize=(8,8,2))
@time ba[301:500, 201:400, 101:110, :] = a
@time b = ba[301:500, 201:400, 101:110, :]

@show size(a)
@show size(b)
@assert all(a.==b)
