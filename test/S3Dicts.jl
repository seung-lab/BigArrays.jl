using Base.Test 
using BigArrays
using S3Dicts

d = S3Dict( "s3://seunglab/jpwu/test/image/4_4_40/" )
ba = BigArray(d)

@show ba.chunkSize
@show ndims(ba)
@show size(ba)
@show eltype(ba)

a = rand(UInt8, 256,256,16)

@testset "test 3D image reading and saving" begin 
    # ba = BigArray(d, UInt8, (128,128,8))
    @time ba[257:512, 257:512, 17:32] = a
    # BigArrays.mysetindex!(ba, a, (201:400, 201:400, 161:116))
    @time b = ba[257:512, 257:512, 17:32]
    @show a[:][end-10:end]
    @show b[:][end-10:end]
    @test all(a.==b)
end 

@testset "test single voxel indexing" begin 
    x = a[1,1,1]
    y = ba[257,257,17]
    @show x
    @show y
    @test x==y
end 

@testset "test segmenation" begin 
    d = S3Dict( "s3://seunglab/jpwu/test/segmentation/4_4_40/" )
    # ba = BigArray( d, configDict )
    @show d
    ba = BigArray(d)

    @show ba.chunkSize
    @show ndims(ba)
    @show size(ba)
    @show eltype(ba)
end 


@testset "test 3D image reading and saving" begin 
    a = rand(UInt32, 256,256,16)
    @time ba[257:512, 257:512, 17:32] = a
    @time b = ba[257:512, 257:512, 17:32]
    @test all(a.==b)
end 

@testset "test segmenation with uint64" begin 
    d = S3Dict( "s3://seunglab/jpwu/test/segmentation-uint64/4_4_40/" )
    @show d
    ba = BigArray(d)

    @show ba.chunkSize
    @show ndims(ba)
    @show size(ba)
    @show eltype(ba)
end 

@testset "test 3D image reading and saving" begin 
    a = rand(UInt64, 256,256,16)
    @time ba[257:512, 257:512, 17:32] = a
    @time b = ba[257:512, 257:512, 17:32]
    @test all(a.==b)
end 


@testset "test affinity map" begin 
    d = S3Dict( "s3://seunglab/jpwu/test/affinitymap/4_4_40/" )
    ba = BigArray(d)

    a = rand(Float32, 256,256,16,3)
    @time ba[257:512, 257:512, 17:32, 1:3] = a
    @time b = ba[257:512, 257:512, 17:32, 1:3]

    @show size(a)
    @show size(b)
    @test all(a.==b)
end 

@testset "test semantic map" begin 
    d = S3Dict( "s3://seunglab/jpwu/test/semanticmap/4_4_40/" )
    a = rand(Float32, 256,256,16,4)
    ba = BigArray(d)

    @time ba[257:512, 257:512, 17:32, 1:4] = a
    @time b = ba[257:512, 257:512, 17:32, 1:4]

    @show size(a)
    @show size(b)
    @test all(a.==b)
end 
