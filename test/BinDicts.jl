using BigArrays
using BigArrays.BinDicts
using Base.Test

# prepare directory
tempDir = tempname()
datasetDir = joinpath(tempDir, "6_6_30")
mkdir(tempDir)
mkdir(datasetDir)
infoString = """
{"num_channels": 1, "type": "image", "data_type": "uint8", "scales": [{"encoding": "gzip", "chunk_sizes": [[100, 100, 5]], "key": "6_6_30", "resolution": [6, 6, 30], "voxel_offset": [0, 0, 0], "size": [12286, 11262, 2046]}]} 
"""
f = open(joinpath(tempDir, "info"), "w")
write(f, infoString)
close(f)

@testset "test BinDict" begin 
    h = BinDict(datasetDir)
    a = rand(UInt8, 20)
    h["test"] = a
    b = h["test"]
    @show a
    @show b
    @test all(a.==b)
end # testset 

@testset "test IO of BigArray with backend of BinDict" begin
    ba = BigArray( BinDict(datasetDir) )
    a = rand(UInt8, 200,200,10)
    ba[201:400, 201:400, 101:110] = a
    b = ba[201:400, 201:400, 101:110]
    @test all(a.==b)
    rm(tempDir; recursive=true)
end # end of testset

