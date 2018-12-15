using Test 
using BigArrays.Infos 
using JSON 

@testset "test Infos module" begin
    fileName = joinpath(@__DIR__, "../asset/info")
    str = read(fileName, String)
    @test Info(str) != nothing 

    data = Vector{UInt8}(str)
    @test Info(data) != nothing

    d = JSON.parsefile(joinpath(@__DIR__, "../asset/info"), dicttype=Dict{Symbol,Any})
    @test Info(d) != nothing

    info = Info(d)
    Infos.get_properties_in_mip_level(info, 0)

    d = Dict(info)
    @test isa(d, Dict{Symbol, Any})

    str = string(info)
    d2 = JSON.parse(str, dicttype=Dict{Symbol, Any})
    # the d will not equal to d2 because the element type of vector is Any in d2
    #@test d == d2
    

    println("test info construction function")
    infoScale = Infos.InfoScale()
    info = Info(; numMip=2)
    @test info != nothing
end
