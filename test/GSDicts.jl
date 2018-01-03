@everywhere using Base.Test 
@everywhere using BigArrays
@everywhere using GSDicts
@everywhere using OffsetArrays

d = GSDict( "gs://seunglab/jpwu/test/image/4_4_40/" );
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
    @test all(a.==b |> parent)
end

@testset "test single voxel indexing" begin 
    x = a[1,1,1]
    y = ba[257,257,17] |>parent 
    @test x==y[1]
end 

@testset "test UInt32 segmentation" begin 
    d = GSDict( "gs://seunglab/jpwu/test/segmentation/4_4_40/" )
    # ba = BigArray( d, configDict )
    ba = BigArray(d)
    a = rand(UInt32, 256,256,16)
    @time ba[257:512, 257:512, 17:32] = a
    @time b = ba[257:512, 257:512, 17:32]
    @test all(a.==b |> parent)
end 


@testset "test UInt64 segmenation with uint64" begin 
    d = GSDict( "gs://seunglab/jpwu/test/segmentation-uint64/4_4_40/" )
    ba = BigArray(d)
    a = rand(UInt64, 256,256,16)
    @time ba[257:512, 257:512, 17:32] = a
    @time b = ba[257:512, 257:512, 17:32]
    @test all(a.==b |> parent)
end 


@testset "test affinity map" begin 
    d = GSDict( "gs://seunglab/jpwu/test/affinitymap/4_4_40/" )
    ba = BigArray(d)

    a = rand(Float32, 256,256,16,3)
    @time ba[257:512, 257:512, 17:32, 1:3] = a
    @time b = ba[257:512, 257:512, 17:32, 1:3]

    @show size(a)
    @show size(b)
    @test all(a.==b |> parent)
end 

@testset "test semantic map" begin 
    d = GSDict( "gs://seunglab/jpwu/test/semanticmap/4_4_40/" )
    a = rand(Float32, 256,256,16,4)
    ba = BigArray(d)

    @time ba[257:512, 257:512, 17:32, 1:4] = a
    @time b = ba[257:512, 257:512, 17:32, 1:4]

    @show size(a)
    @show size(b)
    @test all(a.==b |> parent)
end 
