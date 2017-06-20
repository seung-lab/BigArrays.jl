using BigArrays
using GSDicts

d = GSDict( "gs://seunglab/jpwu/test/image/4_4_40/" )
@show d
ba = BigArray(d)

@show ba.chunkSize
@show ndims(ba)
@show size(ba)
@show eltype(ba)

info("\n test 3D image reading and saving...")
a = rand(UInt8, 200,200,10)
# ba = BigArray(d, UInt8, (128,128,8))
ba[501:700, 501:700, 121:130] = a
# BigArrays.mysetindex!(ba, a, (201:400, 201:400, 101:110))
b = ba[501:700, 501:700, 121:130]
@assert all(a.==b)

info("\n test single voxel indexing ...")
x = a[100,100,5]
y = ba[600,600,125]
@show x
@show y
@assert x==y

# test segmenation
d = GSDict( "gs://seunglab/jpwu/test/segmentation/4_4_40/" )
# ba = BigArray( d, configDict )
@show d
ba = BigArray(d)

@show ba.chunkSize
@show ndims(ba)
@show size(ba)
@show eltype(ba)

info("\n test 3D image reading and saving...")
a = rand(UInt32, 200,200,10)
# ba = BigArray(d, UInt8, (128,128,8))
ba[501:700, 501:700, 121:130] = a
# BigArrays.mysetindex!(ba, a, (201:400, 201:400, 101:110))
b = ba[501:700, 501:700, 121:130]
@assert all(a.==b)

# test segmenation with uint64
d = GSDict( "gs://seunglab/jpwu/test/segmentation-uint64/4_4_40/" )
# ba = BigArray( d, configDict )
@show d
ba = BigArray(d)

@show ba.chunkSize
@show ndims(ba)
@show size(ba)
@show eltype(ba)

info("\n test 3D image reading and saving...")
a = rand(UInt64, 200,200,10)
# ba = BigArray(d, UInt8, (128,128,8))
ba[501:700, 501:700, 121:130] = a
# BigArrays.mysetindex!(ba, a, (201:400, 201:400, 101:110))
b = ba[501:700, 501:700, 121:130]
@assert all(a.==b)



# test affinity map
info("\n\n test affinity map reading and saving...")
d = GSDict( "gs://seunglab/jpwu/test/affinitymap/4_4_40/" )
ba = BigArray(d)

a = rand(Float32, 200,200,10,3)
ba[501:700, 501:700, 121:130, 1:3] = a
b = ba[501:700, 501:700, 121:130, 1:3]

@show size(a)
@show size(b)
@assert all(a.==b)


# test semantic map
info("\n\n test semantic map reading and saving...")
d = GSDict( "gs://seunglab/jpwu/test/semanticmap/4_4_40/" )
a = rand(Float32, 200,200,10,4)
ba = BigArray(d)

ba[401:600, 401:600, 121:130, 1:4] = a
b = ba[401:600, 401:600, 121:130, 1:4]

@show size(a)
@show size(b)
@assert all(a.==b)
