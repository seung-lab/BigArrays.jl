using BigArrays.GSDicts
using Test

# test storage utility first
#include(joinpath(dirname(@__FILE__), "google_cloud/storage_util.jl"))

@testset "test gsdict io" begin 
    kv  = GSDict("gs://seunglab/jpwu/test/image/4_4_40")

    println("test fakekey error capture")
    try 
        kv["fakekey"]
    catch err 
        if isa(err, KeyError)
            println("get normal KeyError: $(err)")
        else
            println("get an error: ", err)
            println("error type: ", typeof(err))
            rethrow()
        end
    end

    a = rand(UInt8, 50)

    kv["test"] = a
    b = kv["test"]
    b = reinterpret(UInt8, b)

    println("make sure that the value saved in the cloud is the same with local")
    @test all(a .== b)
    @test true == haskey(kv, "test")
    @info("delete the file in google cloud storage")
    delete!(kv, "test")
    
    println("test no such key error...")
    try 
        kv["test"]
    catch err 
        @test isa(err, KeyError)
    end 

end 
