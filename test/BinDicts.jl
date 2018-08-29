using BigArrays
using BigArrays.BinDicts
using Test
using OffsetArrays 

# prepare directory
tempDir = tempname()
@show tempDir
datasetDir = joinpath(tempDir, "6_6_30")
mkdir(tempDir)
mkdir(datasetDir)
infoString = """
{"num_channels": 1, "type": "image", "data_type": "uint8", "scales": [
{"encoding": "gzip", "chunk_sizes": [[100, 100, 5]], "key": "6_6_30", "resolution": [6, 6, 30], "voxel_offset": [-300, -300, -10], "size": [510, 510, 2022]}, 
{"encoding": "gzip", "chunk_sizes": [[100, 100, 5]], "key": "12_12_30", "resolution": [12, 12, 30], "voxel_offset": [-597, -597, -103], "size": [12286, 11262, 2046]} 
]} 
"""

write( joinpath(tempDir, "info"), infoString )

@testset "test BinDict" begin 
    h = BinDict(datasetDir)
    a = rand(UInt8, 20)
    h["test"] = a
    b = h["test"]
    @assert haskey(h, "test")
    @assert !haskey(h, "notexist")
    @show a
    @show b
    @test all(a.==b)
end # testset 

@testset "test negative coordinate" begin 
    ba = BigArray( BinDict(datasetDir) )
    @show CartesianRange(ba)
    a = rand(UInt8, 200,200,2000)
    ba[-199:0, -99:100, -4:1995] = a
    b = ba[-199:0, -99:100, -4:1995] 
    @test all(a.==parent(b))
end # end of testset

@testset "test aligned IO crossing the volume boundary" begin 
    ba = BigArray( BinDict(datasetDir) )
    a = rand(UInt8, 200,200,10)
    # respect the volume size, the chunk range over volume size will not be written
    ba[101:300, 101:300, 2006:2015] = a
    b = ba[101:300, 101:300, 2006:2015]
    c = zeros(a)
    c[1:110, 1:110, 1:7] = a[1:110, 1:110, 1:7]
    @test all(c.==parent(b))
end # end of testset

@testset "test nonaligned IO crossing the volume boundary" begin 
    ba = BigArray( BinDict(datasetDir) )
    a = rand(UInt8, 190,190,9)
    # respect the volume size, the chunk range over volume size will not be written
    ba[101:290, 101:290, 2006:2014] = a
    b = ba[101:290, 101:290, 2006:2014]
    c = zeros(a)
    c[1:110, 1:110, 1:7] = a[1:110, 1:110, 1:7]
    @test all(c.==parent(b))
end # end of testset


infoString = replace(infoString, "gzip", "zstd")
write( joinpath(tempDir, "info"), infoString )

@testset "test IO of BigArray with backend of BinDict with zstd compression" begin
    ba = BigArray( BinDict(datasetDir) )
    a = rand(UInt8, 200,200,10)
    ba[1:200, 1:200, 1:10] = a
    b = ba[1:200, 1:200, 1:10]
    @test all(a.==parent(b))
end # end of testset

infoString = replace(infoString, "zstd", "blosclz")
write( joinpath(tempDir, "info"), infoString )


@testset "test merge function with backend of BinDict with blosclz compression" begin
    ba = BigArray( BinDict(datasetDir) )
    a = rand(UInt8, 200,200,10)
    @inbounds merge(ba, OffsetArray(a, 1:200, 1:200, 1:10))
    @inbounds b = ba[1:200, 1:200, 1:10]
    @test all(parent(a).==parent(b))
end # end of testset

infoString = replace(infoString, "blosclz", "zstd")
write( joinpath(tempDir, "info"), infoString )

@testset "test dataset not aligned starting from 0" begin 
    datasetDir = joinpath(tempDir, "12_12_30") 
    mkdir(datasetDir)
    ba = BigArray( BinDict(datasetDir) )
    a = rand(UInt8, 200,200,10)
    ba[204:403, 204:403, 103:112] = a
    b = ba[204:403, 204:403, 103:112] 
    @test all(a.==parent(b))
end # end of testset

@testset "test dataset not aligned starting from 0 and negative coordinates" begin 
    datasetDir = joinpath(tempDir, "12_12_30") 
    ba = BigArray( BinDict(datasetDir) )
    a = rand(UInt8, 200,200,10)
    ba[-96:103, -296:-97, -2:7] = a
    b = ba[-96:103, -296:-97, -2:7] 
    @test all(a.==parent(b))
end # end of testset


# clean the temporary directory
rm(tempDir; recursive=true)
