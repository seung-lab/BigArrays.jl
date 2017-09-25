using BigArrays
using Base.Test

@testset "test index transformation" begin
    str = "2968-3480_1776-2288_16912-17424"
    r = BigArrays.Indexes.string2unit_range( str )
    @test r == [2968:3480, 1776:2288, 16912:17424]
end 
