# import BigArrays: get_config
using BigArrays
using S3Dicts



d = S3Dict( "s3://seunglab/jpwu/tmp/img/" )
ba = BigArray( d )
@show size(ba)
@show eltype(ba)

info("\n test 3D image reading and saving...")
a = rand(UInt8, 200,200,10)
ba = BigArray{S3Dict, UInt8, 3}(d, UInt8, (8,8,2))
ba[201:400, 201:400, 101:110] = a
# BigArrays.mysetindex!(ba, a, (201:400, 201:400, 101:110))
b = ba[201:400, 201:400, 101:110]
@assert all(a.==b)


# test affinity map
info("\n\n test affinity map reading and saving...")
d = S3Dict() "s3://seunglab/jpwu//tmp/aff/" )
a = rand(Float32, 200,200,10,3)
ba = BigArray(d, Float32, (16,16,4,3))

ba[201:400, 201:400, 101:110, 1:3] = a
b = ba[201:400, 201:400, 101:110, 1:3]

@show size(a)
@show size(b)
@assert all(a.==b)
