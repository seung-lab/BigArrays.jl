# import BigArrays: get_config
using BigArrays
using GSDicts

d = GSDict( "gs://jpwu/test.bigarray.img/" )
# configDict = get_config_dict(d)
# ba = BigArray( d, configDict )
@show d
ba = BigArray(d)

@show ba.chunkSize
@show get_config_dict(d)
@show ndims(ba)
@show size(ba)
@show eltype(ba)

info("\n test 3D image reading and saving...")
a = rand(UInt8, 200,200,10)
# ba = BigArray(d, UInt8, (128,128,8))
ba[201:400, 201:400, 101:110] = a
# BigArrays.mysetindex!(ba, a, (201:400, 201:400, 101:110))
b = ba[201:400, 201:400, 101:110]
@assert all(a.==b)


# test affinity map
info("\n\n test affinity map reading and saving...")
d = GSDict( "gs://jpwu/test.bigarray.aff/" )
a = rand(Float32, 200,200,10,3)
ba = BigArray(d, Float32, (128,128,8,3))

ba[201:400, 201:400, 101:110, 1:3] = a
b = ba[201:400, 201:400, 101:110, 1:3]

@show size(a)
@show size(b)
@assert all(a.==b)