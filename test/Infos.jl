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
end
