using BigArrays.S3Dicts
using Test
using Libz

@testset "test s3 IO" begin 
    as3  = S3Dict("s3://seunglab/jpwu/test/image/4_4_40")

    a = rand(UInt8, 50)

    println("save object")
    as3["test"] = a
    println("get object")
    b = as3["test"]
    @test all(a.==b)
    @test haskey(as3, "test") == true
    @test haskey(as3, "test2") == false
    @info("delete the file in s3")
    delete!(as3, "test")
    
    println("test no such key error...")
    try 
        as3["test"]
    catch err 
        @test isa(err, KeyError)
    end 
end 
